.PHONY: test

test:
	go test -mod=readonly -v -tags test -race ./...

