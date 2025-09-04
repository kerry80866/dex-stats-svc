PROTOC        := protoc
PROTOC_GEN_GO := $(shell which protoc-gen-go)
PROTOC_GEN_GRPC_GO := $(shell which protoc-gen-go-grpc)

raft-event:
	@mkdir -p pb
	@echo "Generating raft_event.proto → pb/"
	$(PROTOC) \
		--proto_path=proto \
		--go_out=pb --go_opt=paths=source_relative \
		--go-grpc_out=pb --go-grpc_opt=paths=source_relative \
		proto/raft_event.proto

proto-event:
	@mkdir -p pb
	@echo "Generating event.proto → pb/"
	$(PROTOC) \
		--proto_path=proto \
		--go_out=pb --go_opt=paths=source_relative \
		proto/event.proto

ingest-query:
	@mkdir -p pb
	@echo "Generating ingest_query.proto → pb/"
	$(PROTOC) \
      --proto_path=proto \
      --go_out=pb --go_opt=paths=source_relative \
      --go-grpc_out=pb --go-grpc_opt=paths=source_relative \
      proto/ingest_query.proto

token-meta:
	@mkdir -p pb
	@echo "Generating tokenmeta.proto → pb/"
	$(PROTOC) \
		--proto_path=proto \
		--go_out=pb --go_opt=paths=source_relative \
		--go-grpc_out=pb --go-grpc_opt=paths=source_relative \
		proto/tokenmeta.proto

snapshot:
	@mkdir -p pb
	@echo "Generating snapshot.proto → pb/"
	$(PROTOC) \
		--proto_path=proto \
		--go_out=pb --go_opt=paths=source_relative \
		proto/snapshot.proto

stats-service:
	@mkdir -p pb
	@echo "Generating stats-service.proto → pb/"
	$(PROTOC) \
      --proto_path=proto \
      --go_out=pb --go_opt=paths=source_relative \
      --go-grpc_out=pb --go-grpc_opt=paths=source_relative \
      proto/stats_service.proto

clean:
	@echo "Cleaning generated .pb.go files..."
	@find pb -name "*.pb.go" -delete
