CHANGELOG
=========

## v2.1.4 (23.03.2020)
- Replaced std encoding/json package with the https://github.com/json-iterator/go 
- Added BORS and GHA support
- Added new exponential backoff mechanism for AMQP and Beanstalk modules
- Fixed few concurrency issues with goroutines usage in loops
- Other under-the-hood improvements with tests and code simplify
- RR 1.7.0