package mongodb

import (
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tycheg/reasonr-engine/port"
	"github.com/tycheg/reasonr-engine/port/registry"
)

// Registration in engine.yaml:
//
//	stores:
//	  main: { type: mongodb, uri: "mongodb://db:27017/?replicaSet=rs0", database: reasonr }
func init() {
	registry.RegisterStore("mongodb", func(n *port.Node, d registry.Deps) (any, error) {
		if err := n.Only("type", "uri", "uri_env", "database"); err != nil {
			return nil, err
		}
		uri, err := n.String("uri", "")
		if err != nil {
			return nil, err
		}
		if env, _ := n.String("uri_env", ""); uri == "" && env != "" {
			if uri = os.Getenv(env); uri == "" {
				return nil, fmt.Errorf("mongodb: the environment variable %s is empty", env)
			}
		}
		if uri == "" {
			return nil, fmt.Errorf("mongodb: set uri or uri_env")
		}
		db, err := n.String("database", "reasonr")
		if err != nil {
			return nil, err
		}
		client, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			return nil, fmt.Errorf("mongodb: %w", err)
		}
		return New(client, db, Options{Logger: d.Logger, Own: true}), nil
	}, registry.PersonalData)
}
