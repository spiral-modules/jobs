clean:
	rm -rf rr-jobs
install: all
	cp rr-jobs /usr/local/bin/rr-jobs
uninstall: 
	rm -f /usr/local/bin/rr-jobs
test:
	go test -v -race -cover
	go test -v -race -cover ./broker/amqp
	go test -v -race -cover ./broker/ephemeral
	go test -v -race -cover ./broker/beanstalk
	go test -v -race -cover ./broker/sqs