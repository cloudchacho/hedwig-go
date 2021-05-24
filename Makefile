.PHONY: test

test: clean
	go test -cover -coverprofile coverage.txt -v -race ./... && \
	cd aws && go test -cover -coverprofile coverage.txt -v -race ./... && cd - && \
	cd jsonschema && go test -cover -coverprofile coverage.txt -v -race ./...

clean:
	find . -name coverage.txt -delete
