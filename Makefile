.PHONY: test

test: clean
	go test -cover -coverprofile coverage.txt -v -race ./... && \
	cd aws && go test -cover -coverprofile coverage.txt -v -race ./... && cd - && \
	cd jsonschema && go test -cover -coverprofile coverage.txt -v -race ./... && cd - && \
	cd protobuf && go test -cover -coverprofile coverage.txt -v -race ./...

proto-compile:
	[ -d /usr/local/lib/protobuf/include/cloudchacho ] || (echo "Ensure github.com/cloudchacho/hedwig is cloned at /usr/local/lib/protobuf/include/cloudchacho/"; exit 2)
	cd protobuf && protoc -I/usr/local/lib/protobuf/include -I. --go_out=. --go_opt=module=github.com/cloudchacho/hedwig-go/protobuf container.proto cloudchacho/hedwig/protobuf/options.proto
	cd protobuf/internal && protoc -I/usr/local/lib/protobuf/include -I. --go_out=. --go_opt=module=github.com/cloudchacho/hedwig-go/protobuf/internal protobuf.proto protobuf_alternate.proto protobuf_bad.proto
	cd examples && protoc -I/usr/local/lib/protobuf/include -I. --go_out=. --go_opt=module=github.com/cloudchacho/hedwig-go/examples schema.proto

clean:
	find . -name coverage.txt -delete
