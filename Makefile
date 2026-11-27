GO ?= go
# The tests run on a live MongoDB (a replica set: transactions) when
# REASONR_TEST_MONGODB is set, otherwise they are skipped: make mongo-up, then
# REASONR_TEST_MONGODB='mongodb://localhost:27018/?directConnection=true' make test

.PHONY: all check fmt-check vet lint test test-race mongo-up mongo-down

all: check

check: fmt-check vet lint test

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet ./...

lint:
	staticcheck ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

mongo-up:
	docker run -d --name reasonr-mongo -p 27018:27017 mongo:7 --replSet rs0 --bind_ip_all
	until docker exec reasonr-mongo mongosh --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; do sleep 1; done
	docker exec reasonr-mongo mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'

mongo-down:
	docker rm -f reasonr-mongo
