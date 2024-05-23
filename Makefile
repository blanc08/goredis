
run: build
	@./bin/goredis

build:
	@go build -o bin/goredis .

test:
	go test . -v -coverprofile=cover.out

coverage:
	go tool cover -html cover.out

benchmark:
	go test . -v -bench=BenchmarkRedisSet -benchmem -benchtime=20000000x -memprofile=mem.out -cpuprofile=cpu.out -run="^$$"

phony:
	test, coverage, benchmark