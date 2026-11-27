package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

// Personal data keys (port.KeyStore, RFC §20): the tenant key is
// rt_tenant_keys {_id: tenant, wrapped}, subject keys are rt_subject_keys
// {_id: {t, type, s}, v: version, wrapped}. A destroyed key is a document
// with wrapped = null (a version tombstone).
//
// In a commit transaction the context carries the session, and keys are
// read and created in it. A key created in a rolled back transaction is gone
// with it, so after a failed attempt the keys used are dropped from the
// cache. The tenant key is created before the transaction (Sealer.Ready).

var _ port.KeyStore = (*Store)(nil)

// keysCtx is the key store context: the session of another client (the
// transaction of an additional store that encrypts with this one's keys,
// decision 47 of RFC §20) is not used in it.
func (s *Store) keysCtx(ctx context.Context) context.Context {
	if sess := mongo.SessionFromContext(ctx); sess != nil && sess.Client() != s.client {
		return mongo.NewSessionContext(ctx, nil)
	}
	return ctx
}

// keyDoc is a subject key document.
type keyDoc struct {
	ID struct {
		Type string `bson:"type"`
		S    string `bson:"s"`
	} `bson:"_id"`
	V       int64  `bson:"v"`
	Wrapped []byte `bson:"wrapped"`
}

func (d *keyDoc) wrapped() port.WrappedKey {
	return port.WrappedKey{Subject: port.SubjectRef{Type: d.ID.Type, ID: d.ID.S}, Version: uint32(d.V), Wrapped: d.Wrapped}
}

// keyID is the _id of a subject key document.
func keyID(tenant port.Tenant, ref port.SubjectRef) bson.D {
	return bson.D{{Key: "t", Value: string(tenant)}, {Key: "type", Value: ref.Type}, {Key: "s", Value: ref.ID}}
}

func keyIDs(tenant port.Tenant, refs []port.SubjectRef) bson.A {
	ids := make(bson.A, len(refs))
	for i, r := range refs {
		ids[i] = keyID(tenant, r)
	}
	return ids
}

// TenantKey returns the wrapped tenant key (port.KeyStore).
func (s *Store) TenantKey(ctx context.Context, tenant port.Tenant) ([]byte, error) {
	ctx = s.keysCtx(ctx)
	var doc struct {
		Wrapped []byte `bson:"wrapped"`
	}
	err := s.db.Collection("rt_tenant_keys").FindOne(ctx, bson.D{{Key: "_id", Value: string(tenant)}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return doc.Wrapped, err
}

// CreateTenantKey stores the tenant key if there is none (port.KeyStore).
func (s *Store) CreateTenantKey(ctx context.Context, tenant port.Tenant, wrapped []byte) ([]byte, error) {
	ctx = s.keysCtx(ctx)
	_, err := s.db.Collection("rt_tenant_keys").InsertOne(ctx, bson.D{{Key: "_id", Value: string(tenant)},
		{Key: "tenant", Value: string(tenant)}, {Key: "wrapped", Value: wrapped}})
	switch {
	case err == nil:
		return wrapped, nil
	case !mongo.IsDuplicateKeyError(err):
		return nil, err
	case mongo.SessionFromContext(ctx) != nil:
		return nil, fmt.Errorf("%w: the tenant key was created concurrently", port.ErrConflict) // the transaction is aborted
	}
	return s.TenantKey(ctx, tenant)
}

// TenantKeys returns the keys of every tenant (port.TenantKeyRotator).
func (s *Store) TenantKeys(ctx context.Context) (map[port.Tenant][]byte, error) {
	cur, err := s.db.Collection("rt_tenant_keys").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var docs []struct {
		ID      string `bson:"_id"`
		Wrapped []byte `bson:"wrapped"`
	}
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make(map[port.Tenant][]byte, len(docs))
	for _, d := range docs {
		out[port.Tenant(d.ID)] = d.Wrapped
	}
	return out, nil
}

// ReplaceTenantKey replaces the wrapped tenant key if it is still old
// (port.TenantKeyRotator).
func (s *Store) ReplaceTenantKey(ctx context.Context, tenant port.Tenant, old, wrapped []byte) (bool, error) {
	r, err := s.db.Collection("rt_tenant_keys").UpdateOne(ctx, bson.D{{Key: "_id", Value: string(tenant)}, {Key: "wrapped", Value: old}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "wrapped", Value: wrapped}}}})
	if err != nil {
		return false, err
	}
	return r.MatchedCount == 1, nil
}

// SubjectKeys returns the keys and tombstones of subjects (port.KeyStore).
func (s *Store) SubjectKeys(ctx context.Context, tenant port.Tenant, refs []port.SubjectRef) ([]port.WrappedKey, error) {
	ctx = s.keysCtx(ctx)
	cur, err := s.db.Collection("rt_subject_keys").Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: keyIDs(tenant, refs)}}}})
	if err != nil {
		return nil, err
	}
	var docs []keyDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.WrappedKey, len(docs))
	for i := range docs {
		out[i] = docs[i].wrapped()
	}
	return out, nil
}

// CreateSubjectKey stores a subject key: version 1, after a tombstone — the
// next one; a valid key is returned as is (port.KeyStore). One pipeline
// update with upsert: the document of a valid key does not change (no
// write, the transaction is not aborted).
func (s *Store) CreateSubjectKey(ctx context.Context, tenant port.Tenant, ref port.SubjectRef, wrapped []byte) (port.WrappedKey, error) {
	ctx = s.keysCtx(ctx)
	dead := bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$wrapped", nil}}}, nil}}}
	var doc keyDoc
	err := s.db.Collection("rt_subject_keys").FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: keyID(tenant, ref)}}, mongo.Pipeline{
		{{Key: "$set", Value: bson.D{
			{Key: "tenant", Value: string(tenant)},
			{Key: "v", Value: bson.D{{Key: "$cond", Value: bson.A{dead, bson.D{{Key: "$add", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$v", 0}}}, 1}}}, "$v"}}}},
			{Key: "wrapped", Value: bson.D{{Key: "$cond", Value: bson.A{dead, bson.D{{Key: "$literal", Value: bson.Binary{Data: wrapped}}}, "$wrapped"}}}},
		}}},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	if err != nil {
		return port.WrappedKey{}, err
	}
	return doc.wrapped(), nil
}

// checkKeys touches in the transaction the documents of the keys the unit of
// work encrypted with (decision 8 of RFC §20): a conditional update
// {_id, v, wrapped ≠ null} → seen. MongoDB transactions do not notice a
// "read — write" conflict, but they do notice a write: a concurrent forget
// gets a write conflict, and a key destroyed earlier does not match the
// filter — ErrConflict, the kernel repeats the command.
//
// Not checked are the keys created by this transaction (it has already
// written their document) and the keys of the guarded subjects — those
// whose data was in the previous version of a document the transaction
// replaced with a version check. A forget of such a subject erases that
// document and increments its version: after it the replace does not match
// the filter, and concurrently with it the replace gets a write conflict.
// The key the unit of work encrypted with is the same as that of the
// previous version's data: reading the document decrypted it and updated
// the cache. So a command over its own data does not write the key document.
func checkKeys(ctx context.Context, db *mongo.Database, tenant port.Tenant, used []port.KeyVersion, guarded map[string]bool) error {
	var models []mongo.WriteModel
	seen := bson.NewObjectID() // always a new value: otherwise the update is not a write
	for _, kv := range used {
		if guarded[kv.Subject.Type+":"+kv.Subject.ID] {
			continue
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: keyID(tenant, kv.Subject)}, {Key: "v", Value: int64(kv.Version)},
				{Key: "wrapped", Value: bson.D{{Key: "$ne", Value: nil}}}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{{Key: "seen", Value: seen}}}}))
	}
	if models == nil {
		return nil
	}
	r, err := db.Collection("rt_subject_keys").BulkWrite(ctx, models)
	if err != nil {
		return err
	}
	if r.MatchedCount != int64(len(models)) {
		return fmt.Errorf("%w: the subject key was destroyed before the commit", port.ErrConflict)
	}
	return nil
}

// destroyKeys destroys subject keys in the forget transaction: the document
// stays as a tombstone with the same version.
func destroyKeys(ctx context.Context, db *mongo.Database, tenant port.Tenant, refs []port.SubjectRef) error {
	_, err := db.Collection("rt_subject_keys").UpdateMany(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: keyIDs(tenant, refs)}}}, {Key: "wrapped", Value: bson.D{{Key: "$ne", Value: nil}}}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "wrapped", Value: nil}}}})
	return err
}
