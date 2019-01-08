all:
	@./build.sh
build:
	@./build.sh all
clean:
	rm -rf rr-jobs
install: all
	cp rr-jobs /usr/local/bin/rr-jobs
uninstall: 
	rm -f /usr/local/bin/rr-jobs
test:
	go test -v -race -cover
	go test -v -race -cover ./cpool
	go test -v -race -cover ./broker/amqp
	go test -v -race -cover ./broker/local
	go test -v -race -cover ./broker/beanstalk
	go test -v -race -cover ./broker/sqs