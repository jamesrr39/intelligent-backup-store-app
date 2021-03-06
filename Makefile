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

