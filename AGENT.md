# reactory-queue -- Server Module Agent Context

## What Is This Module

A message queue abstraction module providing three interchangeable queue backends (BullMQ/Redis, AWS SQS, and in-memory), with a unified QueueProvider interface, gRPC/protobuf support, GraphQL endpoints, and OpenTelemetry instrumentation.

- **Module ID**: `reactory-queue`
- **Namespace**: `reactory`
- **FQN**: `reactory.Queue@1.0.0`
- **Version**: `1.0.0`
- **Priority**: `2`
- **License**: MIT
- **Package**: `reactory-queue` v1.0.0
- **Dependencies**: `core.ReactoryServer@1.0.0`

## Directory Structure

```
reactory-queue/
  index.ts                # ReactoryModuleDefinition entry point
  graph/                  # GraphQL type definitions
  services/
    queue/
      BullMessageQueueService.ts    # BullMQ (Redis-backed) queue
      AWSSQSService.ts              # AWS SQS queue
      InMemoryQueueService.ts       # In-memory queue (dev/test)
      QueueProvider.ts              # Unified queue abstraction
  models/                 # Data models
  forms/                  # UI form schemas
  routes/                 # Express routes
  cli/                    # CLI commands
  schema/                 # Schema definitions
  types/                  # TypeScript type definitions
  views/                  # View templates
  workflow/               # Workflow definitions
  protobuf/               # gRPC/protobuf definitions
  i18n/                   # Translation files
```

## Queue Backends

| Backend | Use Case | Backing Store |
|---|---|---|
| `BullMessageQueueService` | Production default | Redis (via BullMQ) |
| `AWSSQSService` | AWS-native deployments | AWS SQS |
| `InMemoryQueueService` | Development and testing | Memory |

All backends implement a common `QueueProvider` interface, making them interchangeable.

## Dependencies (package.json)

- `@aws-sdk/client-sqs` -- AWS SQS SDK
- `bullmq` -- BullMQ queue library
- `bullmq-otel` -- OpenTelemetry integration for BullMQ

## Key Features

- Unified queue provider abstraction
- Message publish/subscribe patterns
- Job scheduling and retry logic
- OpenTelemetry instrumentation for queue metrics
- gRPC endpoints for cross-service queue operations
- GraphQL API for queue management
