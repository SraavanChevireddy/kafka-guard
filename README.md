# kafka-guard

[![CI](https://github.com/SraavanChevireddy/kafka-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/SraavanChevireddy/kafka-guard/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://adoptium.net/)
[![Spring Boot 3](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen.svg)](https://spring.io/projects/spring-boot)

**A governance gateway for Apache Kafka topic provisioning.** Instead of letting anyone run `kafka-topics --create` with whatever flags they like, route topic requests through kafka-guard: every request is validated against a per-cluster policy, queued for approval, and recorded.

> Status: **early-stage (v0.1).** The policy validator and governance workflow are real and fully tested. It governs the *request*; actually creating the topic on the broker is left to your provisioning step (see [Roadmap](#roadmap)).

---

## The problem

Self-service Kafka is great until the topic list looks like this:

- `Orders_FINAL`, `orders-v2`, `test123`, `johns_scratch` — no naming convention, impossible to govern.
- A topic created with **1 partition / RF 1** that becomes a bottleneck and a data-loss risk.
- `retention.ms=-1` (keep forever) set by accident on a high-volume topic — storage quietly explodes.
- No owner on half the topics, so when one misbehaves nobody knows who to call.

kafka-guard makes topic creation a governed request. You define the rules once per cluster; every request is checked against them and only valid, approved requests proceed.

## What a policy can enforce

Policies are per-cluster (with a wildcard `*` default). Every rule is optional.

| Rule | Rejects when |
|---|---|
| **Name pattern** | the topic name doesn't fully match your regex (e.g. `(orders\|payments)\.[a-z0-9_]+`) |
| **Partition bounds** | partitions below the floor or above the ceiling |
| **Min replication factor** | RF below your durability minimum |
| **Owner required** | no owning team/person declared |
| **Required configs** | a mandatory config (e.g. `retention.ms`) is missing |
| **Forbidden configs** | a banned config (e.g. manual `segment.bytes`) is set |
| **Allowed config values** | e.g. `cleanup.policy` is anything other than `delete`/`compact` |
| **Retention cap** | `retention.ms` exceeds the max — or is `-1` (infinite) when a cap exists |

Validation collects **every** violation, not just the first, so requesters get one clear list to fix. The validator ([`TopicPolicyValidator`](src/main/java/com/sraavan/kafkaguard/policy/TopicPolicyValidator.java)) is pure and Spring-free — exhaustively unit-tested.

## Quick start

```bash
docker compose up --build      # Postgres + kafka-guard on :8090
```

Swagger UI: `http://localhost:8090/swagger-ui.html` · Health: `http://localhost:8090/actuator/health`

## Walkthrough

```bash
# 1. Define the policy for the "prod" cluster
curl -XPUT localhost:8090/api/clusters/prod/policy -H 'Content-Type: application/json' -d '{
  "updatedBy": "data-platform",
  "policy": {
    "namePattern": "(orders|payments)\\.[a-z0-9_]+",
    "minPartitions": 3, "maxPartitions": 24, "minReplicationFactor": 3,
    "requireOwner": true,
    "requiredConfigs": ["retention.ms"],
    "allowedConfigValues": { "cleanup.policy": ["delete", "compact"] },
    "maxRetentionMs": 604800000,
    "forbiddenConfigs": ["segment.bytes"]
  }
}'

# 2. Dry-run validate a request (no side effects) — perfect for a CI check on a PR
curl -XPOST localhost:8090/api/clusters/prod/validate -H 'Content-Type: application/json' -d '{
  "topic": { "name": "BadName", "partitions": 1, "replicationFactor": 1, "configs": {}, "owner": "" }
}'
# -> { "valid": false, "violations": [ { "rule": "NAME_PATTERN", ... }, ... ] }

# 3. Submit a valid request -> queued as PENDING
curl -XPOST localhost:8090/api/clusters/prod/requests -H 'Content-Type: application/json' -d '{
  "requestedBy": "alice",
  "topic": {
    "name": "orders.created", "partitions": 6, "replicationFactor": 3,
    "configs": { "retention.ms": "86400000", "cleanup.policy": "delete" },
    "owner": "team-orders"
  }
}'

# 4. Approve it
curl -XPOST localhost:8090/api/requests/1/approve -H 'Content-Type: application/json' -d '{ "approver": "bob" }'

# 5. Audit history for the cluster
curl localhost:8090/api/clusters/prod/requests
```

An invalid submission is stored as `REJECTED_INVALID` (returned with **422**) so even rejected attempts are auditable; a valid one becomes `PENDING` until a human approves or rejects it.

## API

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/api/clusters/{cluster}/policy` | Set/update a policy (`*` for default) |
| `GET` | `/api/clusters/{cluster}/policy` | View the effective policy |
| `POST` | `/api/clusters/{cluster}/validate` | Dry-run validate (no write) |
| `POST` | `/api/clusters/{cluster}/requests` | Submit a topic request |
| `GET` | `/api/clusters/{cluster}/requests` | Request / audit history |
| `POST` | `/api/requests/{id}/approve` | Approve a pending request |
| `POST` | `/api/requests/{id}/reject` | Reject a pending request |

## Architecture

```
policy/    pure validator — TopicPolicy, TopicRequest, TopicPolicyValidator
service/   KafkaGuardService — resolve policy, validate, queue, decide (transactional)
domain/    JPA entities: ClusterPolicy, TopicRequestRecord
repo/      Spring Data repositories
web/       REST controller + DTOs + RFC-7807 errors
db/migration/  Flyway schema (Postgres)
```

## Tech

Java 21 · Spring Boot 3.3 · Spring Data JPA · Flyway · PostgreSQL · springdoc/OpenAPI · JUnit 5 · Docker.

## Roadmap

Honest v0.1. Where a production deployment would go next:

- [ ] An executor that actually creates approved topics via the Kafka AdminClient
- [ ] AuthN (OIDC) so `requestedBy`/`approver` come from verified identity
- [ ] Drift detection: compare live broker topics against approved policy and flag violations
- [ ] A GitHub Action wrapping `/validate` so topic configs are gated in PRs
- [ ] Alter-topic (not just create) governance, with compatibility rules for partition increases
- [ ] Pagination and search on request history

Issues and PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) © Sraavan Chevireddy
