package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

type timerDoc struct {
	Tenant   string `bson:"tenant"`
	ID       bson.D `bson:"_id"`
	FireAt   int64  `bson:"fire_at"`
	Seq      int64  `bson:"seq"`
	Attempts int    `bson:"attempts"`
	Entity   string `bson:"entity_type"`
	EntityID string `bson:"entity_id"`
	Command  string `bson:"command"`
	Args     string `bson:"args"`
	Sealed   string `bson:"sealed"` // the ciphertexts of personal arguments
	// Offset is the scenario clock offset, ns (RFC §27.5): the scenario
	// moment is fire_at + time_offset; older documents lack the field — 0.
	Offset int64 `bson:"time_offset"`
}

// Lease leases fired timers one by one with an atomic findOneAndUpdate: a
// timer leased by another instance is not selected (port.TimerStore).
func (s *Store) Lease(ctx context.Context, req port.LeaseRequest) ([]port.LeasedTimerRecord, error) {
	now := req.Now.UnixNano()
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	filter := bson.D{{Key: "dead", Value: nil}, {Key: "fire_at", Value: bson.D{{Key: "$lte", Value: now}}},
		{Key: "lease_until", Value: bson.D{{Key: "$lte", Value: now}}}}
	var update any = bson.D{{Key: "$set", Value: bson.D{{Key: "lease_until", Value: req.Now.Add(req.TTL).UnixNano()}}},
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}}}
	if req.Scenario {
		// By the scenario clock (a time offset in a test or a demo): the
		// moment and the lease use the timer's offset.
		offset := bson.D{{Key: "$ifNull", Value: bson.A{"$time_offset", int64(0)}}}
		plus := func(field string) bson.D { return bson.D{{Key: "$add", Value: bson.A{field, offset}}} }
		filter = bson.D{{Key: "dead", Value: nil}, {Key: "$expr", Value: bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: "$lte", Value: bson.A{plus("$fire_at"), now}}},
			bson.D{{Key: "$lte", Value: bson.A{plus("$lease_until"), now}}},
		}}}}}
		update = mongo.Pipeline{{{Key: "$set", Value: bson.D{
			{Key: "lease_until", Value: bson.D{{Key: "$subtract", Value: bson.A{req.Now.Add(req.TTL).UnixNano(), offset}}}},
			{Key: "attempts", Value: bson.D{{Key: "$add", Value: bson.A{"$attempts", 1}}}},
		}}}}
	}
	if req.Tenant != "" {
		filter = append(filter, bson.E{Key: "tenant", Value: string(req.Tenant)})
	}
	var out []port.LeasedTimerRecord
	for len(out) < limit {
		var doc timerDoc
		err := s.db.Collection("rt_timers").FindOneAndUpdate(ctx, filter, update,
			options.FindOneAndUpdate().SetSort(bson.D{{Key: "fire_at", Value: 1}, {Key: "seq", Value: 1}}).
				SetReturnDocument(options.After)).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, doc.record())
	}
	if req.Scenario { // in the order of the scenario moment, as the timers fire
		sort.SliceStable(out, func(i, j int) bool { return out[i].At.Add(out[i].Offset).Before(out[j].At.Add(out[j].Offset)) })
	}
	return out, nil
}

// record is the timer of a document.
func (d *timerDoc) record() port.LeasedTimerRecord {
	var id [16]byte
	for _, e := range d.ID {
		if raw, ok := e.Value.(bson.Binary); ok && e.Key == "id" {
			copy(id[:], raw.Data)
		}
	}
	r := port.LeasedTimerRecord{Tenant: port.Tenant(d.Tenant), Attempts: d.Attempts, TimerRecord: port.TimerRecord{
		ID: id, At: time.Unix(0, d.FireAt).UTC(), Offset: time.Duration(d.Offset), Entity: d.Entity, EntityID: []byte(d.EntityID),
		Command: d.Command, Args: []byte(d.Args)}}
	if d.Sealed != "" {
		r.Sealed = []byte(d.Sealed)
	}
	return r
}

func timerKey(tenant port.Tenant, id [16]byte) bson.D {
	return bson.D{{Key: "_id", Value: docID(tenant, bson.Binary{Data: id[:]})}}
}

// Retry releases the lease until at (port.TimerStore).
func (s *Store) Retry(ctx context.Context, tenant port.Tenant, id [16]byte, at time.Time) error {
	_, err := s.db.Collection("rt_timers").UpdateOne(ctx, timerKey(tenant, id), bson.D{{Key: "$set", Value: bson.D{{Key: "lease_until", Value: at.UnixNano()}}}})
	return err
}

// Dead moves a timer to the dead ones (port.TimerStore).
func (s *Store) Dead(ctx context.Context, tenant port.Tenant, id [16]byte, reason string) error {
	_, err := s.db.Collection("rt_timers").UpdateOne(ctx, timerKey(tenant, id), bson.D{{Key: "$set", Value: bson.D{{Key: "dead", Value: reason}}}})
	return err
}

// Revive returns dead timers to work; tenant "" — of all.
func (s *Store) Revive(ctx context.Context, tenant port.Tenant) (int64, error) {
	filter := bson.D{{Key: "dead", Value: bson.D{{Key: "$ne", Value: nil}}}}
	if tenant != "" {
		filter = append(filter, bson.E{Key: "tenant", Value: string(tenant)})
	}
	r, err := s.db.Collection("rt_timers").UpdateMany(ctx, filter, bson.D{{Key: "$set", Value: bson.D{
		{Key: "dead", Value: nil}, {Key: "attempts", Value: 0}, {Key: "lease_until", Value: int64(0)}}}})
	if err != nil {
		return 0, err
	}
	return r.ModifiedCount, nil
}

// Read returns the units of work after from in increasing position order
// (port.Outbox). A position is issued before the transaction, so a
// commit with a smaller position may arrive later: at a gap the read stops
// while the position is in progress (outbox.go).
func (s *Store) Read(ctx context.Context, from port.Position, limit int) ([]port.OutboxRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	cur, err := s.db.Collection("rt_outbox").Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: int64(from)}}}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	type outboxDoc struct {
		ID      int64  `bson:"_id"`
		Tenant  string `bson:"tenant"`
		Payload string `bson:"payload"`
	}
	var docs []outboxDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	var out []port.OutboxRecord
	next := int64(from) + 1
	for _, doc := range docs {
		if doc.ID != next {
			closed, err := s.gapClosed(ctx, next, doc.ID)
			if err != nil {
				return nil, err
			}
			if !closed {
				break
			}
		}
		out = append(out, port.OutboxRecord{Tenant: port.Tenant(doc.Tenant), Position: port.Position(doc.ID), Payload: []byte(doc.Payload)})
		next = doc.ID + 1
	}
	return out, nil
}

// Wait waits for an outbox entry after after (port.Outbox), polling
// every 20 ms.
func (s *Store) Wait(ctx context.Context, after port.Position) error {
	for {
		n, err := s.db.Collection("rt_outbox").CountDocuments(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: int64(after)}}}},
			options.Count().SetLimit(1))
		if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Apply applies the rows of asynchronous projections of the unit at
// position pos and erases the data of forgotten subjects in their
// documents (port.AsyncProjections). A row is applied by its version
// (committer.applyRow), so positions apply in any order and a repeat
// changes nothing; Applied is the largest applied position. The main store
// destroys the keys.
func (s *Store) Apply(ctx context.Context, tenant port.Tenant, pos port.Position, writes []port.RowWrite, deletes []port.RowDelete,
	forget []port.SubjectRef, forgetTables []*port.Table, rewrite port.Rewriter) error {
	sess, err := s.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)
	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		c := &committer{s: s, tenant: tenant, u: &port.UnitRecord{}}
		for i := range writes {
			if err := c.applyRow(ctx, pos, writes[i].Table, &writes[i].Row); err != nil {
				return nil, err
			}
		}
		for i := range deletes {
			if err := c.applyRow(ctx, pos, deletes[i].Table, &port.Row{ID: deletes[i].ID, Version: ^uint64(0)}); err != nil {
				return nil, err
			}
		}
		if len(forget) > 0 && len(forgetTables) > 0 {
			if err := erase(ctx, s.db, tenant, forget, forgetTables, rewrite); err != nil {
				return nil, err
			}
		}
		_, err := s.db.Collection("rt_applied").UpdateOne(ctx, bson.D{{Key: "_id", Value: string(tenant)}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "tenant", Value: string(tenant)}}}, {Key: "$max", Value: bson.D{{Key: "position", Value: int64(pos)}}}},
			options.UpdateOne().SetUpsert(true))
		return nil, err
	})
	if isConflict(err) {
		return fmt.Errorf("%w: %v", port.ErrConflict, err)
	}
	return err
}

// Applied returns the applied position (port.AsyncProjections).
func (s *Store) Applied(ctx context.Context, tenant port.Tenant) (port.Position, error) {
	var doc struct {
		Position int64 `bson:"position"`
	}
	err := s.db.Collection("rt_applied").FindOne(ctx, bson.D{{Key: "_id", Value: string(tenant)}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	return port.Position(doc.Position), err
}

// Append writes events to ev_events; a repeat (position, entity, event) is
// skipped (port.EventLog). The engine encrypted the personal arguments; the
// log is not decrypted when read (RFC §20).
func (s *Store) Append(ctx context.Context, events []port.EventRecord) error {
	var docs []any
	for _, e := range events {
		doc := bson.D{{Key: "_id", Value: bson.D{{Key: "p", Value: int64(e.Position)}, {Key: "e", Value: e.Entity},
			{Key: "s", Value: e.Seq}}}, {Key: "tenant", Value: string(e.Tenant)}, {Key: "entity_type", Value: e.Type},
			{Key: "entity_id", Value: string(e.ID)}, {Key: "version", Value: int64(e.Version)}, {Key: "name", Value: e.Name},
			{Key: "args", Value: string(e.Args)}, {Key: "at", Value: e.At}}
		if len(e.Sealed) > 0 {
			doc = append(doc, bson.E{Key: "sealed", Value: string(e.Sealed)})
		}
		docs = append(docs, doc)
	}
	if len(docs) == 0 {
		return nil
	}
	_, err := s.db.Collection("ev_events").InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil && onlyDuplicates(err) {
		return nil
	}
	return err
}

// onlyDuplicates reports that every insert error is a repeated key.
func onlyDuplicates(err error) bool {
	var bwe mongo.BulkWriteException
	if !errors.As(err, &bwe) || bwe.WriteConcernError != nil {
		return false
	}
	for _, we := range bwe.WriteErrors {
		if we.Code != 11000 {
			return false
		}
	}
	return true
}

// Cursor, SetCursor, Acquire, Bury, DeadLetters, Resurrect — port.Coordinator.

func (s *Store) Cursor(ctx context.Context, consumer string) (port.Position, error) {
	var doc struct {
		Position int64 `bson:"position"`
	}
	err := s.db.Collection("rt_cursors").FindOne(ctx, bson.D{{Key: "_id", Value: consumer}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	return port.Position(doc.Position), err
}

func (s *Store) SetCursor(ctx context.Context, consumer string, pos port.Position) error {
	_, err := s.db.Collection("rt_cursors").ReplaceOne(ctx, bson.D{{Key: "_id", Value: consumer}},
		bson.D{{Key: "position", Value: int64(pos)}}, options.Replace().SetUpsert(true))
	return err
}

func (s *Store) Acquire(ctx context.Context, name, holder string, now time.Time, ttl time.Duration) (bool, error) {
	_, err := s.db.Collection("rt_leases").UpdateOne(ctx,
		bson.D{{Key: "_id", Value: name}, {Key: "$or", Value: bson.A{
			bson.D{{Key: "holder", Value: holder}}, bson.D{{Key: "until", Value: bson.D{{Key: "$lte", Value: now.UnixNano()}}}}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "holder", Value: holder}, {Key: "until", Value: now.Add(ttl).UnixNano()}}}},
		options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return false, nil // another one holds the lease
	}
	return err == nil, err
}

func deadKey(d port.DeadLetter) bson.D {
	return bson.D{{Key: "_id", Value: bson.D{{Key: "c", Value: d.Consumer}, {Key: "p", Value: int64(d.Position)}, {Key: "i", Value: d.Item}}}}
}

func (s *Store) Bury(ctx context.Context, d port.DeadLetter) error {
	_, err := s.db.Collection("rt_dead_letters").UpdateOne(ctx, deadKey(d), bson.D{{Key: "$setOnInsert", Value: bson.D{
		{Key: "consumer", Value: d.Consumer}, {Key: "position", Value: int64(d.Position)}, {Key: "item", Value: d.Item},
		{Key: "tenant", Value: string(d.Tenant)}, {Key: "reason", Value: d.Reason}, {Key: "at", Value: d.At}}}},
		options.UpdateOne().SetUpsert(true))
	return err
}

func (s *Store) DeadLetters(ctx context.Context, consumer string) ([]port.DeadLetter, error) {
	filter := bson.D{}
	if consumer != "" {
		filter = bson.D{{Key: "consumer", Value: consumer}}
	}
	cur, err := s.db.Collection("rt_dead_letters").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "position", Value: 1}, {Key: "item", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var docs []struct {
		Consumer string    `bson:"consumer"`
		Position int64     `bson:"position"`
		Item     int       `bson:"item"`
		Tenant   string    `bson:"tenant"`
		Reason   string    `bson:"reason"`
		At       time.Time `bson:"at"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.DeadLetter, len(docs))
	for i, d := range docs {
		out[i] = port.DeadLetter{Consumer: d.Consumer, Position: port.Position(d.Position), Item: d.Item,
			Tenant: port.Tenant(d.Tenant), Reason: d.Reason, At: d.At.UTC()}
	}
	return out, nil
}

func (s *Store) Resurrect(ctx context.Context, d port.DeadLetter) error {
	_, err := s.db.Collection("rt_dead_letters").DeleteOne(ctx, deadKey(d))
	return err
}

// DropTenant deletes the tenant's data in every collection except the
// outbox, including the personal data keys.
func (s *Store) DropTenant(ctx context.Context, tenant port.Tenant) error {
	names, err := s.db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return err
	}
	for _, n := range names {
		if n == "rt_outbox" || !strings.HasPrefix(n, "e_") && !strings.HasPrefix(n, "p_") && !strings.HasPrefix(n, "rt_") && n != "ev_events" {
			continue
		}
		if _, err := s.db.Collection(n).DeleteMany(ctx, bson.D{{Key: "tenant", Value: string(tenant)}}); err != nil {
			return err
		}
	}
	return nil
}
