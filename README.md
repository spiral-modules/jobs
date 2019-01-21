# RoadRunner: Async Job Pipeline
[![Latest Stable Version](https://poser.pugx.org/spiral/jobs/version)](https://packagist.org/packages/spiral/jobs)
[![GoDoc](https://godoc.org/github.com/spiral/jobs?status.svg)](https://godoc.org/github.com/spiral/jobs)
[![Build Status](https://travis-ci.org/spiral/jobs.svg?branch=master)](https://travis-ci.org/spiral/jobs)
[![Go Report Card](https://goreportcard.com/badge/github.com/spiral/jobs)](https://goreportcard.com/report/github.com/spiral/jobs)
[![Codecov](https://codecov.io/gh/spiral/jobs/branch/master/graph/badge.svg)](https://codecov.io/gh/spiral/jobs/)

## Features
- supports in memory queue, Beanstalk, AMQP, AWS SQS
- can work as standalone application or as part of RoadRunner server
- multiple pipelines per application
- durable (prefetch control, graceful exit, reconnects)
- automatic queue configuration
- plug-and-play PHP library (framework agnostic)
- delayed jobs
- job level timeouts, retries, retry delays
- PHP and Golang consumers and producers
- per pipeline stop/resume
- interactive stats, events, RPC
- works on Windows