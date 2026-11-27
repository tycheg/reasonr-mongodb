package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// Commit writes a unit of work in a transaction (port.RowStore). The
// outbox position is issued by a counter before the transaction
// (outbox.go): an atomic update of one document does not conflict with
// transactions, and the outbox reader does not jump over positions still in
// progress. The key check and forget follow RFC §20.
func (s *Store) Commit(ctx context.Context, tenant port.Tenant, u *port.UnitRecord) (port.Position, error) {
	sess, err := s.client.StartSession()
	if err != nil {
		return 0, err
	}
	defer sess.EndSession(ctx)
	var pos, timerSeq int64
	if u.Outbox != nil {
		// The timer scheduling order is outside the transaction too: the
		// numbers have gaps but do not conflict.
		if n := int64(len(u.Timers)); n > 0 {
			if timerSeq, err = s.counter(ctx, "timers", n); err != nil {
				return 0, err
			}
			timerSeq -= n
		}
		if pos, err = s.allocate(ctx); err != nil {
			return 0, err
		}
		defer s.release(pos)
	}
	res, err := sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		c := &committer{s: s, tenant: tenant, u: u, pos: pos, timerSeq: timerSeq}
		return c.run(ctx)
	})
	if err != nil {
		if isConflict(err) {
			return 0, fmt.Errorf("%w: %v", port.ErrConflict, err)
		}
		return 0, err
	}
	return res.(port.Position), nil
}

// isConflict is a write conflict the driver's retries did not resolve.
func isConflict(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && (se.HasErrorLabel("TransientTransactionError") || se.HasErrorCode(112))
}

type committer struct {
	s      *Store
	tenant port.Tenant
	u      *port.UnitRecord
	pos    int64 // the outbox position issued before the transaction
	// timerSeq is the number after which the timer scheduling numbers go.
	timerSeq int64
	// guarded are the subjects ("Type:id") of the previous versions of the
	// documents the unit of work replaced with a version check: their keys
	// are not checked (checkKeys).
	guarded map[string]bool
}

func (c *committer) coll(name string) *mongo.Collection { return c.s.db.Collection(name) }

func (c *committer) run(ctx context.Context) (port.Position, error) {
	u, tenant := c.u, string(c.tenant)
	if err := c.newKeys(ctx); err != nil {
		return 0, err
	}
	if u.Fired != nil {
		r, err := c.coll("rt_timers").DeleteOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, bson.Binary{Data: u.Fired[:]})}})
		if err != nil {
			return 0, err
		}
		if r.DeletedCount == 0 {
			return 0, port.ErrTimerGone
		}
	}
	if u.Idempotency != "" && u.Result != nil {
		var old struct {
			Expires time.Time `bson:"expires_at"`
		}
		err := c.coll("rt_idempotency").FindOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, u.Idempotency)}}).Decode(&old)
		switch {
		case err == nil && old.Expires.After(u.Now):
			return 0, port.ErrDuplicateKey
		case err != nil && !errors.Is(err, mongo.ErrNoDocuments):
			return 0, err
		}
	}
	var pos port.Position
	if u.Outbox != nil {
		for i := range u.Writes {
			w := &u.Writes[i]
			if w.Projection {
				if err := c.projection(ctx, w); err != nil {
					return 0, err
				}
				continue
			}
			if err := c.write(ctx, w.Table, w.Row, w.Row.Version > 0, w.Row.Version, w.Row.Version+1); err != nil {
				return 0, err
			}
		}
		for _, d := range u.Deletes {
			id, err := value(d.Table.ID, d.ID)
			if err != nil {
				return 0, err
			}
			if _, err := c.coll(collName(d.Table)).DeleteOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, id)}}); err != nil {
				return 0, err
			}
		}
		if err := c.timers(ctx); err != nil {
			return 0, err
		}
		if err := checkKeys(ctx, c.s.db, c.tenant, u.Keys, c.guarded); err != nil {
			return 0, err
		}
		if len(u.Forget) > 0 {
			if err := destroyKeys(ctx, c.s.db, c.tenant, u.Forget); err != nil {
				return 0, err
			}
			if err := erase(ctx, c.s.db, c.tenant, u.Forget, u.ForgetTables, u.Rewrite); err != nil {
				return 0, err
			}
		}
		n := c.pos
		pos = port.Position(n)
		if _, err := c.coll("rt_outbox").InsertOne(ctx, bson.D{{Key: "_id", Value: n}, {Key: "tenant", Value: tenant},
			{Key: "payload", Value: string(u.Outbox)}}); err != nil {
			return 0, err
		}
		if err := c.insertRequests(ctx, n); err != nil {
			return 0, err
		}
	}
	if u.Idempotency != "" && u.Result != nil {
		result, err := u.Result(pos)
		if err != nil {
			return 0, err
		}
		if _, err := c.coll("rt_idempotency").ReplaceOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, u.Idempotency)}},
			bson.D{{Key: "tenant", Value: tenant}, {Key: "result", Value: string(result)}, {Key: "expires_at", Value: u.Expires}},
			options.Replace().SetUpsert(true)); err != nil {
			return 0, err
		}
	}
	return pos, nil
}

// newKeys writes the subject keys the unit created: version 1 or the next
// one over the tombstone of the previous version; a key another
// transaction wrote first is ErrConflict (the command repeats with it).
func (c *committer) newKeys(ctx context.Context) error {
	for _, k := range c.u.NewKeys {
		id := keyID(c.tenant, k.Subject)
		if k.Version == 1 {
			_, err := c.coll("rt_subject_keys").InsertOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: string(c.tenant)},
				{Key: "v", Value: int64(1)}, {Key: "wrapped", Value: bson.Binary{Data: k.Wrapped}}})
			if mongo.IsDuplicateKeyError(err) {
				return fmt.Errorf("%w: the subject key %s %s was created concurrently", port.ErrConflict, k.Subject.Type, k.Subject.ID)
			}
			if err != nil {
				return err
			}
			continue
		}
		r, err := c.coll("rt_subject_keys").UpdateOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "v", Value: int64(k.Version - 1)}, {Key: "wrapped", Value: nil}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int64(k.Version)}, {Key: "wrapped", Value: bson.Binary{Data: k.Wrapped}}}}})
		if err != nil {
			return err
		}
		if r.MatchedCount == 0 {
			return fmt.Errorf("%w: the subject key %s %s was created concurrently", port.ErrConflict, k.Subject.Type, k.Subject.ID)
		}
	}
	return nil
}

// write writes a row with the version next: a new one — insert, an
// existing one — replace with a check of the previous version version; a
// unique index violation is *port.UniqueKeyViolation.
func (c *committer) write(ctx context.Context, t *port.Table, r port.Row, existed bool, version, next uint64) error {
	doc, err := document(c.tenant, t, r, next)
	if err != nil {
		return err
	}
	coll := c.coll(collName(t))
	filter := bson.D{{Key: "_id", Value: doc[0].Value}, {Key: "version", Value: int64(version)}}
	if existed && t.Personal {
		// The previous subjects of the document — for the key check (checkKeys).
		var old struct {
			Subj []string `bson:"_subj"`
		}
		err := coll.FindOneAndReplace(ctx, filter, doc, options.FindOneAndReplace().SetProjection(bson.D{{Key: "_subj", Value: 1}})).Decode(&old)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
			return port.ErrConflict
		case err != nil:
			return unique(err, t)
		}
		for _, s := range old.Subj {
			if c.guarded == nil {
				c.guarded = map[string]bool{}
			}
			c.guarded[s] = true
		}
		return nil
	}
	if existed {
		res, err := coll.ReplaceOne(ctx, filter, doc)
		if err != nil {
			return unique(err, t)
		}
		if res.MatchedCount == 0 {
			return port.ErrConflict
		}
		return nil
	}
	if _, err := coll.InsertOne(ctx, doc); err != nil {
		return unique(err, t)
	}
	return nil
}

// unique: a duplicate _id is a conflict (another one created the row), a
// duplicate of a unique index u_… is UniqueKeyViolation.
func unique(err error, t *port.Table) error {
	if !mongo.IsDuplicateKeyError(err) {
		return err
	}
	msg := err.Error()
	for i := range t.Indexes {
		if ix := &t.Indexes[i]; ix.Unique && strings.Contains(msg, "index: "+indexName(t, ix)+" ") {
			return &port.UniqueKeyViolation{Type: t.Type, Index: ix.Name}
		}
	}
	return port.ErrConflict
}

// projection writes a synchronous projection row whole with its stored version + 1.
func (c *committer) projection(ctx context.Context, w *port.RowWrite) error {
	version, exists, err := c.version(ctx, w.Table, w.Row.ID)
	if err != nil {
		return err
	}
	return c.write(ctx, w.Table, w.Row, exists, version, version+1)
}

// version returns the stored version of a row and its existence.
func (c *committer) version(ctx context.Context, t *port.Table, id port.Scalar) (uint64, bool, error) {
	idv, err := value(t.ID, id)
	if err != nil {
		return 0, false, err
	}
	var doc struct {
		Version int64 `bson:"version"`
	}
	err = c.coll(collName(t)).FindOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, idv)}},
		options.FindOne().SetProjection(bson.D{{Key: "version", Value: 1}})).Decode(&doc)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}
	return uint64(doc.Version), true, nil
}

// applyRow applies a row of an asynchronous projection at position pos by
// row version (RFC §18): only if pos is greater than the row version and
// the version of its deletion mark (rt_deleted). A write sets the row
// version to pos; a deletion (Version == ^0) leaves a mark with version pos.
// A concurrent write of the same row is a transaction conflict.
func (c *committer) applyRow(ctx context.Context, pos port.Position, t *port.Table, r *port.Row) error {
	version, exists, err := c.version(ctx, t, r.ID)
	if err != nil {
		return err
	}
	idv, err := value(t.ID, r.ID)
	if err != nil {
		return err
	}
	mark := bson.D{{Key: "_id", Value: bson.D{{Key: "t", Value: string(c.tenant)}, {Key: "y", Value: t.Type}, {Key: "id", Value: idv}}}}
	var doc struct {
		Version int64 `bson:"version"`
	}
	err = c.coll("rt_deleted").FindOne(ctx, mark).Decode(&doc)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	err = nil
	if uint64(pos) <= max(version, uint64(doc.Version)) {
		return nil
	}
	if r.Version == ^uint64(0) {
		if exists {
			if _, err := c.coll(collName(t)).DeleteOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, idv)}}); err != nil {
				return err
			}
		}
		_, err := c.coll("rt_deleted").ReplaceOne(ctx, mark, bson.D{{Key: "tenant", Value: string(c.tenant)}, {Key: "version", Value: int64(pos)}},
			options.Replace().SetUpsert(true))
		return err
	}
	if err := c.write(ctx, t, *r, exists, version, uint64(pos)); err != nil {
		return err
	}
	if doc.Version > 0 {
		_, err = c.coll("rt_deleted").DeleteOne(ctx, mark)
	}
	return err
}

// timers sets and deletes timers.
func (c *committer) timers(ctx context.Context) error {
	for _, id := range c.u.DeleteTimers {
		if _, err := c.coll("rt_timers").DeleteOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, bson.Binary{Data: id[:]})}}); err != nil {
			return err
		}
	}
	for _, t := range c.u.Timers {
		c.timerSeq++
		if _, err := c.coll("rt_timers").InsertOne(ctx, bson.D{{Key: "_id", Value: docID(c.tenant, bson.Binary{Data: t.ID[:]})},
			{Key: "tenant", Value: string(c.tenant)},
			{Key: "fire_at", Value: t.At.UnixNano()}, {Key: "time_offset", Value: int64(t.Offset)}, // by the real clock, and the scenario offset
			{Key: "seq", Value: c.timerSeq}, {Key: "lease_until", Value: int64(0)},
			{Key: "attempts", Value: 0}, {Key: "dead", Value: nil}, {Key: "entity_type", Value: t.Entity},
			{Key: "entity_id", Value: string(t.EntityID)}, {Key: "command", Value: t.Command}, {Key: "args", Value: string(t.Args)},
			{Key: "sealed", Value: string(t.Sealed)}}); err != nil {
			return err
		}
	}
	return nil
}

// Remembered returns the keyed response (port.RowStore).
func (s *Store) Remembered(ctx context.Context, tenant port.Tenant, key string, now time.Time) ([]byte, error) {
	var doc struct {
		Result  string    `bson:"result"`
		Expires time.Time `bson:"expires_at"`
	}
	err := s.db.Collection("rt_idempotency").FindOne(ctx, bson.D{{Key: "_id", Value: docID(tenant, key)}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) || err == nil && !doc.Expires.After(now) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return []byte(doc.Result), nil
}

// indexName is the index name: u_ for unique ones, i_ for the others.
func indexName(t *port.Table, ix *port.Index) string {
	if ix.Unique {
		return "u_" + collName(t) + "__" + ix.Name
	}
	return "i_" + collName(t) + "__" + ix.Name
}
