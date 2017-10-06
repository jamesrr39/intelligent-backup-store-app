build:
	go build -o bin/intelligent-store cmd/intelligent-store-app-main.go
test:
	go vet ./... && go test ./...
