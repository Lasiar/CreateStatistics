GO=$(shell which go)
GOBUILD=$(GO) build
DATETIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
GITHASH=$(shell git rev-parse HEAD)
VERSION=$(shell cat ver)
LDFLAGS=-ldflags "-s -X main.buildstamp=$(DATETIME) -X main.githash=$(GITHASH) -X main.version=$(VERSION)"

build:
	$(GOBUILD) $(LDFLAGS)
	
