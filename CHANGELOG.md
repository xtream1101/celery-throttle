# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

### Added

- Initial release of Celery Throttle
- Token bucket rate limiting with Redis Lua scripts
- Dynamic queue creation and management
- Support for multiple time units (seconds, minutes, hours)
- Burst allowance for handling traffic spikes
- Custom task processor support
- CLI tool for queue management
- Multi-service isolation with Redis key prefixes
- Named queues for better organization
