package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.EventFeed = (*Store)(nil)

// ReadEvents reads ev_events in _id {p, e, s} order (port.EventFeed): a
// position range is an _id range, so the _id index finds it ({p: N} is less
// than any {p: N, e, s}). The arguments as written, personal ones null.
func (s *Store) ReadEvents(ctx context.Context, after, upto port.Position, limit int) ([]port.EventRecord, error) {
	filter := bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gte", Value: bson.D{{Key: "p", Value: int64(after) + 1}}},
		{Key: "$lt", Value: bson.D{{Key: "p", Value: int64(upto) + 1}}},
	}}}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	cur, err := s.db.Collection("ev_events").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	var docs []struct {
		ID struct {
			P int64 `bson:"p"`
			S int   `bson:"s"`
		} `bson:"_id"`
		Tenant string    `bson:"tenant"`
		Type   string    `bson:"entity_type"`
		EntID  string    `bson:"entity_id"`
		Name   string    `bson:"name"`
		Args   string    `bson:"args"`
		At     time.Time `bson:"at"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.EventRecord, len(docs))
	for i, d := range docs {
		out[i] = port.EventRecord{Tenant: port.Tenant(d.Tenant), Position: port.Position(d.ID.P), Seq: d.ID.S,
			Type: d.Type, Name: d.Name, ID: []byte(d.EntID), At: d.At.UTC(), Args: []byte(d.Args)}
	}
	return out, nil
}
