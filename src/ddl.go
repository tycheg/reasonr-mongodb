package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// SchemaError: the schema cannot change without a manual migration.
type SchemaError struct {
	Model string
	Msg   string
}

func (e *SchemaError) Error() string { return "mongodb: model " + e.Model + ": " + e.Msg }

type schemaDoc struct {
	Model   string `bson:"model"`
	Version int64  `bson:"version"`
	Hash    string `bson:"hash"`
	Schema  string `bson:"schema"`
	// Resealed: the personal data storage of documents is brought to this
	// version (reseal.go).
	Resealed bool `bson:"resealed"`
}

func (s *Store) lastSchema(ctx context.Context, model string) (*schemaDoc, error) {
	var doc schemaDoc
	err := s.db.Collection("rt_schema_versions").FindOne(ctx, bson.D{{Key: "model", Value: model}},
		options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return &doc, err
}

// LastSchema returns the last written model schema (port.Schemas).
func (s *Store) LastSchema(ctx context.Context, model string) ([]byte, error) {
	doc, err := s.lastSchema(ctx, model)
	if err != nil || doc == nil {
		return nil, err
	}
	return []byte(doc.Schema), nil
}

// EnsureSchema creates the indexes of the layout l (unique ones are
// partial: a key with none takes no part), has rewrite recompute the key
// fields of the tenants' documents when the schema changes and writes the
// version (port.Schemas). Duplicates are checked before a unique index.
func (s *Store) EnsureSchema(ctx context.Context, l *port.Layout, tenants []port.Tenant, rewrite port.Rewriter) error {
	sum := sha256.Sum256(l.Schema)
	hash := hex.EncodeToString(sum[:])
	last, err := s.lastSchema(ctx, l.Model)
	if err != nil {
		return err
	}
	changed := last == nil || last.Hash != hash
	if changed && last != nil && len(tenants) > 0 {
		for i := range l.Tables {
			if err := s.backfill(ctx, &l.Tables[i], tenants, rewrite); err != nil {
				return err
			}
		}
	}
	for i := range l.Tables {
		t := &l.Tables[i]
		for j := range t.Indexes {
			if err := s.ensureIndex(ctx, l.Model, t, &t.Indexes[j]); err != nil {
				return err
			}
		}
		if t.Personal { // a subject's documents for forget
			if _, err := s.db.Collection(collName(t)).Indexes().CreateOne(ctx, mongo.IndexModel{
				Keys: bson.D{{Key: "tenant", Value: 1}, {Key: "_subj", Value: 1}}, Options: options.Index().SetName(subjectsIndex(t))}); err != nil {
				return err
			}
		}
	}
	if _, err := s.db.Collection("rt_timers").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "dead", Value: 1}, {Key: "fire_at", Value: 1}, {Key: "seq", Value: 1}}}); err != nil {
		return err
	}
	if _, err := s.db.Collection("rt_requests").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "next_at", Value: 1}, {Key: "seq", Value: 1}}}); err != nil {
		return err
	}
	if changed {
		version := int64(1)
		if last != nil {
			version = last.Version + 1
		}
		_, err := s.db.Collection("rt_schema_versions").InsertOne(ctx, bson.D{{Key: "model", Value: l.Model}, {Key: "version", Value: version},
			{Key: "hash", Value: hash}, {Key: "schema", Value: string(l.Schema)}, {Key: "applied_at", Value: time.Now().UTC()}})
		if err != nil {
			return err
		}
		last = &schemaDoc{Version: version}
	}
	if last.Resealed {
		return nil
	}
	return s.resealModel(ctx, l, last.Version, tenants, rewrite)
}

func (s *Store) ensureIndex(ctx context.Context, model string, t *port.Table, ix *port.Index) error {
	keys := bson.D{{Key: "tenant", Value: 1}}
	for i, c := range ix.Columns {
		dir := 1
		if ix.Desc[i] {
			dir = -1
		}
		keys = append(keys, bson.E{Key: c, Value: dir})
	}
	coll := s.db.Collection(collName(t))
	opts := options.Index().SetName(indexName(t, ix))
	if !ix.Unique {
		keys = append(keys, bson.E{Key: "id", Value: 1})
		return createIndex(ctx, coll, indexName(t, ix), mongo.IndexModel{Keys: keys, Options: opts})
	}
	exists := bson.D{}
	group := bson.D{{Key: "tenant", Value: "$tenant"}}
	for _, c := range ix.Columns {
		exists = append(exists, bson.E{Key: c, Value: bson.D{{Key: "$exists", Value: true}}})
		group = append(group, bson.E{Key: strings.ReplaceAll(c, ".", "_"), Value: "$" + c})
	}
	cur, err := coll.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$match", Value: exists}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: group}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
		{{Key: "$match", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: 1}}}}}},
		{{Key: "$limit", Value: 10}},
	})
	if err != nil {
		return err
	}
	var dups []struct {
		Key bson.D `bson:"_id"`
		N   int    `bson:"n"`
	}
	if err := cur.All(ctx, &dups); err != nil {
		return err
	}
	if len(dups) > 0 {
		var list []string
		for _, d := range dups {
			var tenant any
			var parts []string
			for _, e := range d.Key {
				if e.Key == "tenant" {
					tenant = e.Value
				} else {
					parts = append(parts, fmt.Sprint(e.Value))
				}
			}
			list = append(list, fmt.Sprintf("%v: %s (%d records)", tenant, strings.Join(parts, ", "), d.N))
		}
		return &SchemaError{Model: model, Msg: fmt.Sprintf("the unique index %s.%s cannot be created: repeated values %s",
			t.Type, ix.Name, strings.Join(list, "; "))}
	}
	opts.SetUnique(true).SetPartialFilterExpression(exists)
	return createIndex(ctx, coll, indexName(t, ix), mongo.IndexModel{Keys: keys, Options: opts})
}

// createIndex creates an index; an index with the same name and another
// definition (the key of a field renamed with was is in a new field) is
// recreated.
func createIndex(ctx context.Context, coll *mongo.Collection, name string, im mongo.IndexModel) error {
	_, err := coll.Indexes().CreateOne(ctx, im)
	var se mongo.ServerError
	if !errors.As(err, &se) || !se.HasErrorCode(85) && !se.HasErrorCode(86) { // IndexOptionsConflict, IndexKeySpecsConflict
		return err
	}
	if err := coll.Indexes().DropOne(ctx, name); err != nil {
		return err
	}
	_, err = coll.Indexes().CreateOne(ctx, im)
	return err
}

// backfill recomputes the key fields of the tenants' documents with
// rewrite: from the state by the schema (fields with previous names by was),
// blind ones from the decrypted values.
func (s *Store) backfill(ctx context.Context, t *port.Table, tenants []port.Tenant, rewrite port.Rewriter) error {
	names := make(bson.A, len(tenants))
	for i, tn := range tenants {
		names[i] = string(tn)
	}
	coll := s.db.Collection(collName(t))
	cur, err := coll.Find(ctx, bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: names}}}})
	if err != nil {
		return err
	}
	type found struct {
		tenant string
		row    port.Row
	}
	var all []found
	for cur.Next(ctx) {
		r, err := row(t, cur.Current)
		if err != nil {
			cur.Close(ctx)
			return err
		}
		all = append(all, found{cur.Current.Lookup("tenant").StringValue(), r})
	}
	if err := cur.Close(ctx); err != nil {
		return err
	}
	for _, f := range all {
		out, changed, err := rewrite(ctx, port.Tenant(f.tenant), t, f.row)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		set, unset := bson.D{}, bson.D{}
		for i, k := range t.Keys {
			v, err := value(k.Kind, out.Keys[i])
			if err != nil {
				return err
			}
			if v == nil {
				unset = append(unset, bson.E{Key: k.Name, Value: ""})
			} else {
				set = append(set, bson.E{Key: k.Name, Value: v})
			}
		}
		upd := bson.D{}
		if len(set) > 0 {
			upd = append(upd, bson.E{Key: "$set", Value: set})
		}
		if len(unset) > 0 {
			upd = append(upd, bson.E{Key: "$unset", Value: unset})
		}
		id, err := value(t.ID, f.row.ID)
		if err != nil {
			return err
		}
		if len(upd) > 0 {
			if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: docID(port.Tenant(f.tenant), id)}}, upd); err != nil {
				return err
			}
		}
	}
	return nil
}
