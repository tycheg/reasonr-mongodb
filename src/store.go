// Package mongodb holds the ReasonR engine adapters on MongoDB (RFC
// §30–31): a row store (the byte-level store ports of the engine) — the
// main store, asynchronous projections, timers, the outbox, effect stream
// coordination, the event log and schemas. A replica set is needed: a unit
// of work commits in a transaction.
//
// A row document: _id {t: tenant, id: key}, tenant, id, version, state —
// canonical JSON (a string), sealed and _subj — the ciphertexts of personal
// values and their subjects, k_… — index keys. The key types give MongoDB
// the same order as the model: numbers — int64, decimal and timestamp
// (nanoseconds) — Decimal128, strings — UTF-8 (bytewise comparison), uuid —
// 16 binary bytes; none — no field (sorts first, takes no part in
// uniqueness). bytes and composite values are binary (equality only: the
// engine sorts by them itself, port.Capabilities.OpaqueBytes).
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"unicode"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// Store is a row store on MongoDB.
type Store struct {
	client *mongo.Client
	db     *mongo.Database
	log    *slog.Logger
	own    bool // disconnect the client in Close
}

var (
	_ port.RowStore         = (*Store)(nil)
	_ port.AsyncProjections = (*Store)(nil)
	_ port.TimerStore       = (*Store)(nil)
	_ port.Outbox           = (*Store)(nil)
	_ port.EventLog         = (*Store)(nil)
	_ port.Requests         = (*Store)(nil)
	_ port.Schemas          = (*Store)(nil)
	_ port.KeyStore         = (*Store)(nil)
	_ port.Coordinator      = (*Store)(nil)
)

// Options holds the store settings.
type Options struct {
	Logger *slog.Logger
	// Own: Close disconnects the client.
	Own bool
}

// New returns a store in the database db of the client.
func New(client *mongo.Client, db string, opts Options) *Store {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Store{client: client, db: client.Database(db), log: log, own: opts.Own}
}

// Close disconnects the client if the store owns it.
func (s *Store) Close() error {
	if s.own {
		return s.client.Disconnect(context.Background())
	}
	return nil
}

// Database returns the store's database.
func (s *Store) Database() *mongo.Database { return s.db }

// Capabilities: everything except full-text search; bytes compare for
// equality only.
func (s *Store) Capabilities() port.Capabilities {
	return port.Capabilities{Transactions: true, Unique: true, Conds: port.AllConds, SortDesc: true, Pages: true, Scan: true, Personal: true,
		OpaqueBytes: true}
}

// collName: e_ for entities and actors, p_ for projections; the type name in snake_case.
func collName(t *port.Table) string {
	if t.Projection {
		return "p_" + snake(t.Type)
	}
	return "e_" + snake(t.Type)
}

func snake(s string) string {
	// A full name (Ordering.Order): parts joined with "__" — ordering__order.
	if strings.Contains(s, ".") {
		parts := strings.Split(s, ".")
		for i, p := range parts {
			parts[i] = snake(p)
		}
		return strings.Join(parts, "__")
	}
	var b strings.Builder
	rs := []rune(s)
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(rs[i-1]) || i+1 < len(rs) && unicode.IsLower(rs[i+1])) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// value is the BSON value of a column of kind k; none — nil (the field is not written).
func value(k port.ColumnKind, v port.Scalar) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		if k == port.ColNumeric || k == port.ColNanos {
			return bson.ParseDecimal128(x)
		}
		return x, nil
	case [16]byte:
		return bson.Binary{Subtype: 0, Data: x[:]}, nil
	case []byte:
		return bson.Binary{Data: x}, nil
	}
	return v, nil
}

// scalar reads the BSON value of a column of kind k.
func scalar(k port.ColumnKind, raw bson.RawValue) (port.Scalar, error) {
	switch raw.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return nil, nil
	case bson.TypeInt64:
		return raw.Int64(), nil
	case bson.TypeInt32:
		return int64(raw.Int32()), nil
	case bson.TypeDouble:
		return raw.Double(), nil
	case bson.TypeDecimal128:
		return raw.Decimal128().String(), nil
	case bson.TypeString:
		return raw.StringValue(), nil
	case bson.TypeBoolean:
		return raw.Boolean(), nil
	case bson.TypeBinary:
		_, data := raw.Binary()
		if k == port.ColUUID {
			var u [16]byte
			if len(data) != 16 {
				return nil, fmt.Errorf("mongodb: a uuid of %d bytes", len(data))
			}
			copy(u[:], data)
			return u, nil
		}
		return append([]byte(nil), data...), nil
	}
	return nil, fmt.Errorf("mongodb: a key of BSON type %s", raw.Type)
}

// docID is the _id of a row document.
func docID(tenant port.Tenant, id any) bson.D {
	return bson.D{{Key: "t", Value: string(tenant)}, {Key: "id", Value: id}}
}

// document is a row document with the version version.
func document(tenant port.Tenant, t *port.Table, r port.Row, version uint64) (bson.D, error) {
	id, err := value(t.ID, r.ID)
	if err != nil {
		return nil, err
	}
	d := bson.D{{Key: "_id", Value: docID(tenant, id)}, {Key: "tenant", Value: string(tenant)}, {Key: "id", Value: id},
		{Key: "version", Value: int64(version)}, {Key: "state", Value: string(r.State)}}
	if t.Personal {
		d = append(d, bson.E{Key: "sealed", Value: string(r.Sealed)}, bson.E{Key: "_subj", Value: subjectTexts(r.Subjects)})
	}
	for i, k := range t.Keys {
		if i >= len(r.Keys) {
			break
		}
		v, err := value(k.Kind, r.Keys[i])
		if err != nil {
			return nil, err
		}
		if v != nil {
			d = append(d, bson.E{Key: k.Name, Value: v})
		}
	}
	return d, nil
}

// row reads a row document with its key fields.
func row(t *port.Table, raw bson.Raw) (port.Row, error) {
	var r port.Row
	id, err := scalar(t.ID, raw.Lookup("id"))
	if err != nil {
		return r, err
	}
	r.ID = id
	if v, ok := raw.Lookup("version").AsInt64OK(); ok {
		r.Version = uint64(v)
	}
	r.State = []byte(raw.Lookup("state").StringValue())
	if s, ok := raw.Lookup("sealed").StringValueOK(); ok && s != "" {
		r.Sealed = []byte(s)
	}
	if arr, ok := raw.Lookup("_subj").ArrayOK(); ok {
		vals, _ := arr.Values()
		texts := make([]string, 0, len(vals))
		for _, v := range vals {
			texts = append(texts, v.StringValue())
		}
		r.Subjects = subjectRefs(texts)
	}
	r.Keys = make([]port.Scalar, len(t.Keys))
	for i, k := range t.Keys {
		if r.Keys[i], err = scalar(k.Kind, raw.Lookup(k.Name)); err != nil {
			return r, err
		}
	}
	return r, nil
}

// Load returns a row (port.RowReader).
func (s *Store) Load(ctx context.Context, tenant port.Tenant, t *port.Table, id port.Scalar) (port.Row, bool, error) {
	idv, err := value(t.ID, id)
	if err != nil {
		return port.Row{}, false, err
	}
	raw, err := s.db.Collection(collName(t)).FindOne(ctx, bson.D{{Key: "_id", Value: docID(tenant, idv)}}).Raw()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return port.Row{}, false, nil
	}
	if err != nil {
		return port.Row{}, false, err
	}
	r, err := row(t, raw)
	return r, err == nil, err
}

// NextSequence allocates n sequence numbers (port.RowStore).
func (s *Store) NextSequence(ctx context.Context, tenant port.Tenant, typ string, n int) (int64, error) {
	last, err := s.counter(ctx, "seq:"+string(tenant)+":"+typ, int64(n))
	return last - int64(n) + 1, err
}

// counter increments the rt_counters counter by n and returns the new value.
func (s *Store) counter(ctx context.Context, name string, n int64) (int64, error) {
	var doc struct {
		Value int64 `bson:"value"`
	}
	err := s.db.Collection("rt_counters").FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: name}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "value", Value: n}}}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	return doc.Value, err
}

// Find searches rows by key fields (port.RowReader); a page is keyset
// pagination after Query.After.
func (s *Store) Find(ctx context.Context, tenant port.Tenant, q *port.Query) ([]port.Row, error) {
	if q.Limit == 0 {
		return nil, nil
	}
	filter, err := query(tenant, q)
	if err != nil {
		return nil, err
	}
	opts := options.Find()
	if len(q.Order) > 0 {
		var sortDoc bson.D
		for _, o := range q.Order {
			dir := 1
			if o.Desc {
				dir = -1
			}
			sortDoc = append(sortDoc, bson.E{Key: o.Column, Value: dir})
		}
		opts.SetSort(sortDoc)
	}
	if q.Limit > 0 {
		opts.SetLimit(int64(q.Limit))
	}
	cur, err := s.db.Collection(collName(q.Table)).Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: search %s: %w", q.Table.Type, err)
	}
	defer cur.Close(ctx)
	var out []port.Row
	for cur.Next(ctx) {
		r, err := row(q.Table, cur.Current)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, cur.Err()
}

var never = bson.D{{Key: "$expr", Value: false}}

// query builds the filter of a search.
func query(tenant port.Tenant, q *port.Query) (bson.D, error) {
	t := q.Table
	kinds := map[string]port.ColumnKind{"id": t.ID}
	for _, k := range t.Keys {
		kinds[k.Name] = k.Kind
	}
	and := bson.A{bson.D{{Key: "tenant", Value: string(tenant)}}}
	for _, c := range q.Conds {
		kind, ok := kinds[c.Column]
		if !ok {
			return nil, fmt.Errorf("mongodb: %s has no field %s", t.Type, c.Column)
		}
		cond, err := condition(c.Column, kind, c)
		if err != nil {
			return nil, err
		}
		and = append(and, cond)
	}
	if q.After != nil {
		var alts bson.A
		for i, o := range q.Order {
			var conj bson.A
			for j := range i {
				p, err := value(kinds[q.Order[j].Column], q.After[j])
				if err != nil {
					return nil, err
				}
				conj = append(conj, bson.D{{Key: q.Order[j].Column, Value: p}})
			}
			p, err := value(kinds[o.Column], q.After[i])
			if err != nil {
				return nil, err
			}
			switch {
			case p == nil && !o.Desc:
				conj = append(conj, bson.D{{Key: o.Column, Value: bson.D{{Key: "$ne", Value: nil}}}})
			case p == nil && o.Desc:
				conj = append(conj, never)
			case !o.Desc:
				conj = append(conj, bson.D{{Key: o.Column, Value: bson.D{{Key: "$gt", Value: p}}}})
			default:
				conj = append(conj, bson.D{{Key: "$or", Value: bson.A{
					bson.D{{Key: o.Column, Value: bson.D{{Key: "$lt", Value: p}}}}, bson.D{{Key: o.Column, Value: nil}}}}})
			}
			alts = append(alts, bson.D{{Key: "$and", Value: conj}})
		}
		and = append(and, bson.D{{Key: "$or", Value: alts}})
	}
	return bson.D{{Key: "$and", Value: and}}, nil
}

// condition translates a condition with the semantics of the engine's
// matching: none equals only none, an order comparison with none is false.
func condition(col string, kind port.ColumnKind, c port.Cond) (bson.D, error) {
	if c.Op == port.OpIn {
		var vals bson.A
		for _, x := range c.Values {
			p, err := value(kind, x)
			if err != nil {
				return nil, err
			}
			vals = append(vals, p) // nil also matches a missing field
		}
		return bson.D{{Key: col, Value: bson.D{{Key: "$in", Value: vals}}}}, nil
	}
	var x port.Scalar
	if len(c.Values) > 0 {
		x = c.Values[0]
	}
	p, err := value(kind, x)
	if err != nil {
		return nil, err
	}
	switch c.Op {
	case port.OpEq:
		return bson.D{{Key: col, Value: p}}, nil
	case port.OpNe:
		return bson.D{{Key: col, Value: bson.D{{Key: "$ne", Value: p}}}}, nil
	}
	if p == nil {
		return never, nil
	}
	op := map[port.CondOp]string{port.OpLt: "$lt", port.OpLe: "$lte", port.OpGt: "$gt", port.OpGe: "$gte"}[c.Op]
	return bson.D{{Key: col, Value: bson.D{{Key: op, Value: p}}}}, nil
}
