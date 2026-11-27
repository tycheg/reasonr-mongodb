package mongodb

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data in documents (RFC §20): the engine writes a row's state
// without its personal values, their ciphertexts to sealed, the row's
// subjects to _subj (an index: forget finds documents by it), and blind
// values to the key fields on personal fields.

// subjectTexts gives the values of the _subj field: "Type:id".
func subjectTexts(refs []port.SubjectRef) bson.A {
	out := make(bson.A, len(refs))
	for i, r := range refs {
		out[i] = r.Type + ":" + r.ID
	}
	return out
}

// subjectRefs reads the _subj field.
func subjectRefs(texts []string) []port.SubjectRef {
	if texts == nil {
		return nil
	}
	out := make([]port.SubjectRef, len(texts))
	for i, s := range texts {
		t, id, _ := strings.Cut(s, ":")
		out[i] = port.SubjectRef{Type: t, ID: id}
	}
	return out
}

// subjectsIndex is the name of the index on document subjects.
func subjectsIndex(t *port.Table) string { return "s_" + collName(t) + "__subj" }

// erase rewrites the tenant's documents of the tables tables that hold
// values of the subjects refs with rewrite (it erases their ciphertexts and
// blind values) and increments their versions: a concurrent command gets a
// conflict and rereads the data.
func erase(ctx context.Context, db *mongo.Database, tenant port.Tenant, refs []port.SubjectRef, tables []*port.Table, rewrite port.Rewriter) error {
	texts := subjectTexts(refs)
	for _, t := range tables {
		if !t.Personal {
			continue
		}
		coll := db.Collection(collName(t))
		cur, err := coll.Find(ctx, bson.D{{Key: "tenant", Value: string(tenant)}, {Key: "_subj", Value: bson.D{{Key: "$in", Value: texts}}}})
		if err != nil {
			return err
		}
		var hits []port.Row
		for cur.Next(ctx) {
			r, err := row(t, cur.Current)
			if err != nil {
				cur.Close(ctx)
				return err
			}
			hits = append(hits, r)
		}
		if err := cur.Close(ctx); err != nil {
			return err
		}
		for _, r := range hits {
			out, changed, err := rewrite(ctx, tenant, t, r)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			doc, err := document(tenant, t, out, r.Version+1)
			if err != nil {
				return err
			}
			if _, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: doc[0].Value}}, doc); err != nil {
				return err
			}
		}
	}
	return nil
}
