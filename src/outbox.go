package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Outbox positions. The counter is the document rt_counters{_id: "outbox"}:
// value is the last issued position, inflight are the positions whose
// transactions are still running, with the issue moment. Issuing is one
// atomic update of the document outside the transaction: concurrent commits
// do not conflict on it. After the transaction (successful or not) the
// position is removed from inflight.
//
// At a gap g the reader looks at inflight first, then at the outbox: if g
// is in progress — it waits; if not and there is no entry g — the
// transaction rolled back, and the gap is final (a position leaves inflight
// only after the transaction ends, and an outbox entry appears atomically
// with the commit). A position that hangs in inflight longer than
// inflightLimit is considered rolled back: MongoDB aborts transactions
// longer than transactionLifetimeLimitSeconds (60 s by default).
const inflightLimit = 70 * time.Second

// allocate issues the next position and marks it "in progress".
func (s *Store) allocate(ctx context.Context) (int64, error) {
	now := time.Now().UnixNano()
	var doc struct {
		Value int64 `bson:"value"`
	}
	err := s.db.Collection("rt_counters").FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: "outbox"}}, mongo.Pipeline{
		{{Key: "$set", Value: bson.D{{Key: "value", Value: bson.D{{Key: "$add", Value: bson.A{
			bson.D{{Key: "$ifNull", Value: bson.A{"$value", 0}}}, 1}}}}}}},
		{{Key: "$set", Value: bson.D{{Key: "inflight", Value: bson.D{{Key: "$concatArrays", Value: bson.A{
			bson.D{{Key: "$filter", Value: bson.D{
				{Key: "input", Value: bson.D{{Key: "$ifNull", Value: bson.A{"$inflight", bson.A{}}}}},
				{Key: "cond", Value: bson.D{{Key: "$gt", Value: bson.A{"$$this.at", now - int64(inflightLimit)}}}}}}},
			bson.A{bson.D{{Key: "p", Value: "$value"}, {Key: "at", Value: now}}}}}}}}}},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	return doc.Value, err
}

// release removes a position from "in progress" after the transaction.
func (s *Store) release(pos int64) {
	_, err := s.db.Collection("rt_counters").UpdateOne(context.Background(), bson.D{{Key: "_id", Value: "outbox"}},
		bson.D{{Key: "$pull", Value: bson.D{{Key: "inflight", Value: bson.D{{Key: "p", Value: pos}}}}}})
	if err != nil {
		s.log.Warn("mongodb: the outbox position is not removed from in progress", "position", pos, "error", err)
	}
}

// gapClosed reports whether the gap of positions [from, to) is final: none
// is in progress and there are no outbox entries (checked after inflight).
func (s *Store) gapClosed(ctx context.Context, from, to int64) (bool, error) {
	var doc struct {
		Inflight []struct {
			P  int64 `bson:"p"`
			At int64 `bson:"at"`
		} `bson:"inflight"`
	}
	err := s.db.Collection("rt_counters").FindOne(ctx, bson.D{{Key: "_id", Value: "outbox"}}).Decode(&doc)
	if err != nil {
		return false, err
	}
	now := time.Now().UnixNano()
	for _, f := range doc.Inflight {
		if f.P >= from && f.P < to && now-f.At < int64(inflightLimit) {
			return false, nil
		}
	}
	n, err := s.db.Collection("rt_outbox").CountDocuments(ctx, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gte", Value: from}, {Key: "$lt", Value: to}}}}, options.Count().SetLimit(1))
	return n == 0, err
}
