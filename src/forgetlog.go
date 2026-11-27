package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.ForgetLog = (*Store)(nil)

// forgottenDoc is a forget log entry (rt_forget_log).
type forgottenDoc struct {
	Tenant   string    `bson:"tenant"`
	Position int64     `bson:"position"`
	Type     string    `bson:"subject_type"`
	ID       string    `bson:"subject_id"` // the canonical JSON of the identifier
	At       time.Time `bson:"at"`
}

// Record appends forget log entries to rt_forget_log (port.ForgetLog): a
// repeated entry is an upsert of the same document.
func (s *Store) Record(ctx context.Context, entries []port.Forgotten) error {
	if len(entries) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, len(entries))
	for i, e := range entries {
		id := bson.D{{Key: "t", Value: string(e.Tenant)}, {Key: "p", Value: int64(e.Position)},
			{Key: "type", Value: e.Subject.Type}, {Key: "s", Value: e.Subject.ID}}
		models[i] = mongo.NewUpdateOneModel().SetFilter(bson.D{{Key: "_id", Value: id}}).SetUpsert(true).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: forgottenDoc{Tenant: string(e.Tenant), Position: int64(e.Position),
				Type: e.Subject.Type, ID: e.Subject.ID, At: e.At.UTC()}}})
	}
	_, err := s.db.Collection("rt_forget_log").BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// Forgotten returns the forget log entries with a moment not earlier than
// since (port.ForgetLog).
func (s *Store) Forgotten(ctx context.Context, since time.Time) ([]port.Forgotten, error) {
	cur, err := s.db.Collection("rt_forget_log").Find(ctx, bson.D{{Key: "at", Value: bson.D{{Key: "$gte", Value: since.UTC()}}}},
		options.Find().SetSort(bson.D{{Key: "position", Value: 1}, {Key: "tenant", Value: 1}, {Key: "subject_type", Value: 1}, {Key: "subject_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var docs []forgottenDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.Forgotten, len(docs))
	for i, d := range docs {
		out[i] = port.Forgotten{Tenant: port.Tenant(d.Tenant), Position: port.Position(d.Position), At: d.At.UTC(),
			Subject: port.SubjectRef{Type: d.Type, ID: d.ID}}
	}
	return out, nil
}
