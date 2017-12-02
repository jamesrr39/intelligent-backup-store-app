build:
	go build -o bin/intelligent-store cmd/intelligent-store-app-main.go
test:
	go vet ./... && go test ./...
generate_protobufs:
	protoc --go_out=intelligentstore/protobufs proto_files/client_upload.proto
integration_tests:
	go test ./... -run Integration -tags=integration
