run:
	@go run cmd/main.go

proto:
	@protoc \
		--go_out=paths=source_relative:. \
		--go-grpc_out=paths=source_relative:. \
		examplepb/example.proto