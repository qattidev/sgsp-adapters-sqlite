.PHONY: generate test vet check-generated

generate:
	go generate ./...

test:
	go test -race ./...

vet:
	go vet ./...

check-generated: generate
	git diff --exit-code -- internal/dbgen
