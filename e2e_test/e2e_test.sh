#!/bin/bash

set -e
rm -rf ~/tmp/intelligent-store
mkdir -p ~/tmp/intelligent-store
go run cmd/intelligent-store-app-main.go init ~/tmp/intelligent-store/
go run cmd/intelligent-store-app-main.go create-bucket ~/tmp/intelligent-store/ docs
go run cmd/intelligent-store-app-main.go backup-to ~/tmp/intelligent-store/ docs ~/tmp/docs
go run cmd/intelligent-store-app-main.go start-webapp ~/tmp/intelligent-store/
