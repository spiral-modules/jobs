# RoadRunner Job Manager
[![Latest Stable Version](https://poser.pugx.org/spiral/jobs/version)](https://packagist.org/packages/spiral/jobs)
[![Build Status](https://travis-ci.org/spiral/jobs.svg?branch=master)](https://travis-ci.org/spiral/jobs)
[![Codecov](https://codecov.io/gh/spiral/jobs/branch/master/graph/badge.svg)](https://codecov.io/gh/spiral/jobs/)

## Features
- supports in memory queue, Beanstalk, AWS SQS
- can work as standalone application or as part of RoadRunner server
- multiple pipelines per application
- plug-and-play PHP library (framework agnostic)
- combines job and process manager
- delayed jobs
- job level timeouts and retries, retry delays
- PHP and Golang consumers and producers
- pipelines stop/resume
- graceful exit
- works on Windows