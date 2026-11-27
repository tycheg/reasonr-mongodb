package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
)

var _ port.FeatureStore = (*Store)(nil)

// featureDoc is a feature flag rollout rule (rt_features).
type featureDoc struct {
	Feature string `bson:"feature"`
	Tenant  string `bson:"tenant"`
	Role    string `bson:"role"`
	Actor   string `bson:"actor"`
}

// featureLogDoc is an entry of the rollout change log (rt_feature_log).
type featureLogDoc struct {
	At         time.Time `bson:"at"`
	By         string    `bson:"by"`
	Feature    string    `bson:"feature"`
	On         bool      `bson:"on"`
	Tenant     string    `bson:"tenant"`
	Role       string    `bson:"role"`
	Actor      string    `bson:"actor"`
	Everywhere bool      `bson:"everywhere"`
	Seq        int64     `bson:"seq"` // the order of entries with one at
}

func featureID(f string, sc port.FeatureScope) bson.D {
	return bson.D{{Key: "f", Value: f}, {Key: "t", Value: string(sc.Tenant)}, {Key: "r", Value: sc.Role}, {Key: "a", Value: sc.Actor}}
}

// FeatureRules returns the feature flag rollout from rt_features (port.FeatureStore).
func (s *Store) FeatureRules(ctx context.Context) ([]port.FeatureRule, error) {
	cur, err := s.db.Collection("rt_features").Find(ctx, bson.D{},
		options.Find().SetSort(bson.D{{Key: "feature", Value: 1}, {Key: "tenant", Value: 1}, {Key: "role", Value: 1}, {Key: "actor", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var docs []featureDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.FeatureRule, len(docs))
	for i, d := range docs {
		out[i] = port.FeatureRule{Feature: d.Feature, Scope: port.FeatureScope{Tenant: port.Tenant(d.Tenant), Role: d.Role, Actor: d.Actor}}
	}
	return out, nil
}

// ChangeFeature changes the rollout and writes the change to
// rt_feature_log in one transaction (port.FeatureStore).
func (s *Store) ChangeFeature(ctx context.Context, c port.FeatureChange) error {
	sess, err := s.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(ctx)
	sc := c.Scope
	_, err = sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		rules := s.db.Collection("rt_features")
		var err error
		switch {
		case c.On:
			_, err = rules.UpdateOne(ctx, bson.D{{Key: "_id", Value: featureID(c.Feature, sc)}},
				bson.D{{Key: "$setOnInsert", Value: featureDoc{Feature: c.Feature, Tenant: string(sc.Tenant), Role: sc.Role, Actor: sc.Actor}}},
				options.UpdateOne().SetUpsert(true))
		case c.Everywhere:
			_, err = rules.DeleteMany(ctx, bson.D{{Key: "feature", Value: c.Feature}})
		default:
			_, err = rules.DeleteOne(ctx, bson.D{{Key: "_id", Value: featureID(c.Feature, sc)}})
		}
		if err != nil {
			return nil, err
		}
		_, err = s.db.Collection("rt_feature_log").InsertOne(ctx, featureLogDoc{At: c.At.UTC(), By: c.By, Feature: c.Feature, On: c.On,
			Tenant: string(sc.Tenant), Role: sc.Role, Actor: sc.Actor, Everywhere: c.Everywhere, Seq: time.Now().UnixNano()})
		return nil, err
	})
	return err
}

// FeatureLog returns the rollout change log (port.FeatureStore).
func (s *Store) FeatureLog(ctx context.Context, feature string) ([]port.FeatureChange, error) {
	filter := bson.D{}
	if feature != "" {
		filter = bson.D{{Key: "feature", Value: feature}}
	}
	cur, err := s.db.Collection("rt_feature_log").Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "at", Value: 1}, {Key: "seq", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var docs []featureLogDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	out := make([]port.FeatureChange, len(docs))
	for i, d := range docs {
		out[i] = port.FeatureChange{At: d.At.UTC(), By: d.By, Feature: d.Feature, On: d.On, Everywhere: d.Everywhere,
			Scope: port.FeatureScope{Tenant: port.Tenant(d.Tenant), Role: d.Role, Actor: d.Actor}}
	}
	return out, nil
}
