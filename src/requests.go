package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// requestDoc is an external request waiting to be sent (the rt_requests collection).
type requestDoc struct {
	ID       bson.D `bson:"_id"` // the tenant and the idempotency key
	Tenant   string `bson:"tenant"`
	Seq      int64  `bson:"seq"` // the write order: position << 16 | the number in the unit of work
	Position int64  `bson:"position"`
	At       int64  `bson:"at"`      // the time of the unit of work, Unix nanoseconds
	NextAt   int64  `bson:"next_at"` // not selected earlier: a lease or a retry pause
	Attempts int    `bson:"attempts"`
	System   string `bson:"system"`
	Request  string `bson:"request"`
	Args     string `bson:"args"`   // without personal values
	Sealed   string `bson:"sealed"` // their ciphertexts
	Timer    []byte `bson:"timer"`  // the deadline timer; nil — none
}

func requestKey(tenant port.Tenant, key [16]byte) bson.D {
	return docID(tenant, bson.Binary{Data: key[:]})
}

// idParts returns the tenant and the uuid from the _id of a document (docID).
func idParts(id bson.D) (string, [16]byte) {
	var tenant string
	var k [16]byte
	for _, e := range id {
		switch v := e.Value.(type) {
		case string:
			if e.Key == "t" {
				tenant = v
			}
		case bson.Binary:
			if e.Key == "id" {
				copy(k[:], v.Data)
			}
		}
	}
	return tenant, k
}

// insertRequests writes the external requests of a unit of work with the
// position pos in the commit transaction (port.Requests). A request
// with a key that is already waiting to be sent is not written
// ($setOnInsert; a duplicate error would abort the transaction): the key is
// deterministic (the entity, the command, the time, the number), and for
// the recipient it is the same request.
func (c *committer) insertRequests(ctx context.Context, pos int64) error {
	if len(c.u.Requests) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, len(c.u.Requests))
	for i, r := range c.u.Requests {
		var timer any
		if r.Timer != nil {
			timer = bson.Binary{Data: r.Timer[:]}
		}
		d := bson.D{{Key: "_id", Value: requestKey(c.tenant, r.Key)}, {Key: "tenant", Value: string(c.tenant)},
			{Key: "at", Value: c.u.Now.UnixNano()}, {Key: "next_at", Value: int64(0)}, {Key: "attempts", Value: 0},
			{Key: "system", Value: r.System}, {Key: "request", Value: r.Request}, {Key: "args", Value: string(r.Args)},
			{Key: "sealed", Value: string(r.Sealed)}, {Key: "timer", Value: timer}}
		fields := append(d[1:], bson.E{Key: "position", Value: pos}, bson.E{Key: "seq", Value: pos<<16 | int64(i)})
		models[i] = mongo.NewUpdateOneModel().SetFilter(bson.D{d[0]}).SetUpsert(true).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: fields}})
	}
	_, err := c.coll("rt_requests").BulkWrite(ctx, models)
	return err
}

// LeaseRequests leases ready requests one by one with an atomic
// findOneAndUpdate (port.Requests), then reads the deadline timers of
// the leased ones with one query.
func (s *Store) LeaseRequests(ctx context.Context, now time.Time, limit int, ttl time.Duration) ([]port.QueuedRequest, error) {
	if limit <= 0 {
		limit = 100
	}
	filter := bson.D{{Key: "next_at", Value: bson.D{{Key: "$lte", Value: now.UnixNano()}}}}
	var out []port.QueuedRequest
	var docs []requestDoc
	for len(out) < limit {
		var d requestDoc
		err := s.db.Collection("rt_requests").FindOneAndUpdate(ctx, filter,
			bson.D{{Key: "$set", Value: bson.D{{Key: "next_at", Value: now.Add(ttl).UnixNano()}}},
				{Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}}},
			options.FindOneAndUpdate().SetSort(bson.D{{Key: "next_at", Value: 1}, {Key: "seq", Value: 1}}).
				SetReturnDocument(options.After)).Decode(&d)
		if errors.Is(err, mongo.ErrNoDocuments) {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, d.queued())
		docs = append(docs, d)
	}
	var timers bson.A
	for _, d := range docs {
		if d.Timer != nil {
			timers = append(timers, docID(port.Tenant(d.Tenant), bson.Binary{Data: d.Timer}))
		}
	}
	if len(timers) == 0 {
		return out, nil
	}
	cur, err := s.db.Collection("rt_timers").Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: timers}}}},
		options.Find().SetProjection(bson.D{{Key: "fire_at", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var found []struct {
		ID     bson.D `bson:"_id"`
		FireAt int64  `bson:"fire_at"`
	}
	if err := cur.All(ctx, &found); err != nil {
		return nil, err
	}
	type timerKey struct {
		tenant string
		id     [16]byte
	}
	fire := map[timerKey]int64{}
	for _, t := range found {
		tenant, id := idParts(t.ID)
		fire[timerKey{tenant, id}] = t.FireAt
	}
	for i, d := range docs {
		if d.Timer == nil {
			continue
		}
		k := timerKey{tenant: d.Tenant}
		copy(k.id[:], d.Timer)
		if at, ok := fire[k]; ok {
			out[i].Deadline = time.Unix(0, at).UTC()
		} else {
			out[i].TimerGone = true
		}
	}
	return out, nil
}

// queued is the request of a document.
func (d *requestDoc) queued() port.QueuedRequest {
	_, key := idParts(d.ID)
	r := port.QueuedRequest{Tenant: port.Tenant(d.Tenant), Position: port.Position(d.Position), At: time.Unix(0, d.At).UTC(),
		Attempts: d.Attempts, Request: port.RequestRecord{Key: key, System: d.System, Request: d.Request, Args: []byte(d.Args)}}
	if d.Sealed != "" {
		r.Request.Sealed = []byte(d.Sealed)
	}
	return r
}

// RequestDone deletes a request (port.Requests).
func (s *Store) RequestDone(ctx context.Context, tenant port.Tenant, key [16]byte) error {
	_, err := s.db.Collection("rt_requests").DeleteOne(ctx, bson.D{{Key: "_id", Value: requestKey(tenant, key)}})
	return err
}

// RetryRequest schedules the next attempt (port.Requests).
func (s *Store) RetryRequest(ctx context.Context, tenant port.Tenant, key [16]byte, at time.Time, reason string) error {
	_, err := s.db.Collection("rt_requests").UpdateOne(ctx, bson.D{{Key: "_id", Value: requestKey(tenant, key)}}, bson.D{{Key: "$set", Value: bson.D{
		{Key: "next_at", Value: at.UnixNano()}, {Key: "last_error", Value: reason}}}})
	return err
}
