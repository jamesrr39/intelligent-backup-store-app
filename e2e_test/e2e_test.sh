#!/bin/bash

set -e

mkdir ~/tmp/store
go run cmd/intelligent-store-app-main.go init ~/tmp/store/
go run cmd/intelligent-store-app-main.go create-bucket ~/tmp/store/ docs
go run cmd/intelligent-store-app-main.go backup-to ~/tmp/store/ docs ~/tmp/docs
go run cmd/intelligent-store-app-main.go start-webapp ~/tmp/store/
go run cmd/intelligent-store-app-main.go backup-to http://localhost:8080 docs ~/tmp/docs
go run cmd/intelligent-store-app-main.go export ~/tmp/store/ docs ~/tmp/
