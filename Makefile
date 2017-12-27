BINARY=create_statistics
GO=$(shell which go)
GOBUILD=$(GO) build
CURRENTDIR=$(shell pwd)
DATETIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
VERINCREMENT=$(shell bash $(CURRENTDIR)/autoicrement.sh)	
GITHASH=$(shell git rev-parse HEAD)
VERSION=$(shell git describe --abbrev=0 --tags)
LDFLAGS=-ldflags "-s -X main.buildstamp=$(DATETIME) -X main.githash=$(GITHASH) -X main.version=$(VERSION)"


build:
	echo $(VERINCREMENT)	
	$(GOBUILD) $(LDFLAGS) -o $(BINARY)
