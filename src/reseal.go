package mongodb

import (
	"context"
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data migrations (RFC §20): after a new schema version the
// documents of the personal types, and those still holding ciphertexts of
// fields no longer personal, are brought to the model by the engine's
// rewriter: values of
// fields that became personal are encrypted, those that stopped being
// personal are decrypted into state (none for forgotten subjects), those
// renamed with was are re-encrypted under the new name. The resealed mark of
// a version is in rt_schema_versions. Events in the outbox and the log stay
// as they were.

// resealModel rewrites the tenant documents of the model if its schema
// version version is not resealed yet.
func (s *Store) resealModel(ctx context.Context, l *port.Layout, version int64, tenants []port.Tenant, rewrite port.Rewriter) error {
	if len(tenants) > 0 {
		names := make(bson.A, len(tenants))
		for i, t := range tenants {
			names[i] = string(t)
		}
		for i := range l.Tables {
			t := &l.Tables[i]
			// A type that is not personal: its documents with ciphertexts of
			// fields that stopped being personal, every document if it is in
			// Rewrite.
			n, err := s.reseal(ctx, t, names, !t.Personal && !slices.Contains(l.Rewrite, t.Type), !t.Personal, rewrite)
			if err != nil {
				return fmt.Errorf("mongodb: re-encrypting %s: %w", collName(t), err)
			}
			if n > 0 {
				s.log.Info("the personal data is brought to the model", "collection", collName(t), "documents", n)
			}
		}
	}
	_, err := s.db.Collection("rt_schema_versions").UpdateOne(ctx, bson.D{{Key: "model", Value: l.Model}, {Key: "version", Value: version}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "resealed", Value: true}}}})
	return err
}

// reseal rewrites the documents of the collection of t that rewrite
// changes; a document is written with a version check and a new version: a
// concurrent command gets a conflict and repeats, and a document changed
// concurrently is already written by the model. sealedOnly: only documents
// with a sealed field; plain: the type is not personal, and a document with
// a sealed field loses it even if rewrite does not change it. It returns
// the number rewritten.
func (s *Store) reseal(ctx context.Context, t *port.Table, tenants bson.A, sealedOnly, plain bool, rewrite port.Rewriter) (int, error) {
	filter := bson.D{{Key: "tenant", Value: bson.D{{Key: "$in", Value: tenants}}}}
	if sealedOnly {
		filter = append(filter, bson.E{Key: "sealed", Value: bson.D{{Key: "$exists", Value: true}}})
	}
	coll := s.db.Collection(collName(t))
	cur, err := coll.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	n := 0
	for cur.Next(ctx) {
		r, err := row(t, cur.Current)
		if err != nil {
			return n, err
		}
		tenant := port.Tenant(cur.Current.Lookup("tenant").StringValue())
		out, changed, err := rewrite(ctx, tenant, t, r)
		if err != nil {
			return n, err
		}
		_, sealedErr := cur.Current.LookupErr("sealed")
		if !changed && !(plain && sealedErr == nil) { // a document of a type no longer personal loses its sealed and _subj fields
			continue
		}
		doc, err := document(tenant, t, out, r.Version+1)
		if err != nil {
			return n, err
		}
		res, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: doc[0].Value}, {Key: "version", Value: int64(r.Version)}}, doc)
		if err != nil {
			return n, err
		}
		n += int(res.ModifiedCount)
	}
	return n, cur.Err()
}
