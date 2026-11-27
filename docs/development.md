# Development and tests

```bash
docker run -d --name reasonr-mongo -p 27018:27017 mongo:7 --replSet rs0 --bind_ip_all
docker exec reasonr-mongo mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
REASONR_TEST_MONGODB='mongodb://localhost:27018/?directConnection=true' go test ./...
```

Without `REASONR_TEST_MONGODB` the tests are skipped. Each check works in its own database. The tests run the byte-level conformance suites of the contract (`port/porttest`) and the store's own checks (concurrent commits, a unique index over repeated keys). The runs on the example models through the engine — scenarios, migrations of schemas and personal data — are the distribution's (`reasonr-dist`, `integration/mongodb`).

`make check` runs gofmt, vet, staticcheck and the tests; `make mongo-up` starts the replica set above. A release is a tag `vX.Y.Z`.
