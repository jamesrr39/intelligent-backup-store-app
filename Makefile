.PHONY: clean
clean:
	rm -rf build

.PHONY: bundle_static_assets
bundle_static_assets:
	go run vendor/github.com/rakyll/statik/statik.go -src=storewebserver/static -dest=build/client

.PHONY: build
build: clean bundle_static_assets
	go build -tags "prod" -o build/bin/default/intelligent-store cmd/intelligent-store-app-main.go

.PHONY: build_prod_linux_x86_64
build_prod_linux_x86_64: clean bundle_static_assets
	env GOOS=linux GOARCH=amd64 go build -tags "prod" -o build/bin/linux_amd64/intelligent-store cmd/intelligent-store-app-main.go

.PHONY: test
test:
	go vet ./... && go test ./...

.PHONY: generate_protobufs
generate_protobufs:
	protoc --go_out=intelligentstore/protobufs proto_files/client_upload.proto

.PHONY: all_tests
all_tests: test
	go test ./... -run Integration -tags=integration

.PHONY: update_snapshots
update_snapshots:
	UPDATE_SNAPSHOTS=1 go test ./...

.PHONY: install
install: build
	cp build/bin/default/intelligent-store ${shell go env GOBIN}/

LOCALENV_DIR=data/localenv
LOCALENV_BUCKETNAME=test1

.PHONY: ensure_localenv
ensure_localenv:
	mkdir -p ${LOCALENV_DIR}

.PHONY: setup_localenv_store
setup_localenv_store:
	go run cmd/intelligent-store-app-main.go init -C ${LOCALENV_DIR}
	go run cmd/intelligent-store-app-main.go create-bucket -C ${LOCALENV_DIR} ${LOCALENV_BUCKETNAME}

.PHONY: run_dev_webserver
run_dev_webserver: ensure_localenv
#go run cmd/intelligent-store-app-main.go -C ${LOCALENV_DIR} start-webapp
	go run cmd/intelligent-store-app-main.go --store-location=${LOCALENV_DIR} start-webapp
