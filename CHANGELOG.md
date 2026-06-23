# Changelog

All notable changes to this project are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-23

### Added
- Initial public release.
- Topic-policy validator: name pattern, partition bounds, min replication, owner
  requirement, required/forbidden configs, allowed config values, and retention cap
  (including rejection of infinite `retention.ms=-1`).
- Per-cluster and wildcard default policies, stored as JSON.
- Validate (dry-run) and submit endpoints; valid requests queue as PENDING, invalid
  ones are recorded as REJECTED_INVALID with the full violation list.
- Approve / reject workflow with audit retention of every request and decision.
- REST API with OpenAPI/Swagger UI; Docker + docker-compose.
