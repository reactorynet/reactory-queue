# Reactory Queue Services

A flexible and extensible queue service abstraction layer that supports multiple queue backends including BullMQ (Redis), AWS SQS, and in-memory queues.

## Overview

The queue services module provides a unified interface for working with different queue implementations. The `QueueProvider` acts as a central registry and factory for accessing queue services.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  QueueProvider                       │
│  (Central registry and factory)                     │
│                                                      │
│  ┌──────────────────────────────────────────────┐  │
│  │  Provider Registry (Map)                     │  │
│  │  - 'bullmq'    → BullMessageQueueService    │  │
│  │  - 'in-memory' → InMemoryQueueService       │  │
│  │  - 'aws-sqs'   → AWSSQSQueueService         │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │  BullMQ  │    │In-Memory │    │ AWS SQS  │
  │ Service  │    │ Service  │    │ Service  │
  └──────────┘    └──────────┘    └──────────┘
        │                │                │
        ▼                ▼                ▼
  ┌──────────┐    ┌──────────┐    ┌──────────┐
  │  Redis   │    │  Memory  │    │   SQS    │
  └──────────┘    └──────────┘    └──────────┘
```

## Available Queue Providers

### 1. BullMQ (`bullmq`)
- **Backend**: Redis via BullMQ
- **Use Case**: Production workloads, distributed systems
- **Features**: 
  - Persistent storage
  - Job scheduling and retries
  - Distributed worker support
  - Job prioritization
- **Configuration**:
  ```bash
  REACTORY_QUEUE_BULLMQ_ENABLED=true
  REACTORY_REDIS_HOST=localhost
  REACTORY_REDIS_PORT=6379
  REACTORY_REDIS_PASSWORD=reactory
  ```

### 2. In-Memory (`in-memory`)
- **Backend**: JavaScript Map
- **Use Case**: Development, testing, rapid prototyping
- **Features**:
  - Fast, no external dependencies
  - Simple to use
  - Always available as fallback
- **Configuration**: Enabled by default

### 3. AWS SQS (`aws-sqs`)
- **Backend**: Amazon Simple Queue Service
- **Use Case**: AWS-native applications, serverless architectures
- **Features**:
  - Fully managed service
  - Infinite scalability
  - Pay-per-use pricing
- **Configuration**:
  ```bash
  REACTORY_QUEUE_AWS_SQS_ENABLED=true
  AWS_REGION=us-east-1
  DEFAULT_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/myqueue
  ```

## QueueProvider API

### Getting Providers

#### `getProvider(key?: string): QueueServiceType | undefined`
Get a specific queue provider by key.

```typescript
const bullQueue = queueProvider.getProvider('bullmq');
const memQueue = queueProvider.getProvider('in-memory');
const sqsQueue = queueProvider.getProvider('aws-sqs');
```

#### `getDefaultProvider(): QueueServiceType | undefined`
Get the configured default provider.

```typescript
const queue = queueProvider.getDefaultProvider();
```

#### `getAllProviders(): Map<string, QueueServiceType>`
Get all registered providers.

```typescript
const allProviders = queueProvider.getAllProviders();
for (const [key, provider] of allProviders) {
  console.log(`Provider: ${key}, Type: ${provider.provider}`);
}
```

### Managing Providers

#### `registerProvider(key: string, provider: QueueServiceType): void`
Register a custom queue provider.

```typescript
const myCustomQueue = new MyCustomQueueService(props, context);
queueProvider.registerProvider('my-custom', myCustomQueue);
```

#### `unregisterProvider(key: string): boolean`
Remove a provider from the registry.

```typescript
const removed = queueProvider.unregisterProvider('aws-sqs');
```

#### `setDefaultProvider(key: string): void`
Change the default provider.

```typescript
queueProvider.setDefaultProvider('bullmq');
```

### Query Methods

#### `hasProvider(key: string): boolean`
Check if a provider is registered.

```typescript
if (queueProvider.hasProvider('bullmq')) {
  // Use BullMQ
}
```

#### `getAvailableProviders(): string[]`
Get list of all registered provider keys.

```typescript
const available = queueProvider.getAvailableProviders();
// ['bullmq', 'in-memory', 'aws-sqs']
```

## Queue Service Interface

All queue providers implement the `QueueServiceType` interface:

```typescript
interface IQueueService<TEnvelope, TId> {
  // Enqueue a message
  enqueue(message: TEnvelope, options?: any): Promise<TId>;
  
  // Dequeue a message
  dequeue(): Promise<TEnvelope | null>;
  
  // Delete a single message
  deleteMessage(id: TId, options?: DeleteMessageOptions): Promise<void>;
  
  // Delete multiple messages
  deleteMessages(ids: TId[], options?: DeleteMessageOptions): Promise<void>;
  
  // Receive messages
  receiveMessages(options?: ReceiveMessageOptions): Promise<TEnvelope[]>;
  
  // Count messages in queue
  count(queueId?: string): Promise<number>;
  
  // Create a new queue
  createQueue(queueId: string): Promise<boolean>;
  
  // Delete a queue
  deleteQueue(queueId: string): Promise<boolean>;
}
```

## Configuration

### Environment Variables

```bash
# Default provider selection
REACTORY_QUEUE_PROVIDER=in-memory  # or 'bullmq', 'aws-sqs'

# BullMQ Configuration
REACTORY_QUEUE_BULLMQ_ENABLED=true
REACTORY_REDIS_HOST=localhost
REACTORY_REDIS_PORT=6379
REACTORY_REDIS_PASSWORD=reactory

# AWS SQS Configuration
REACTORY_QUEUE_AWS_SQS_ENABLED=true
AWS_REGION=us-east-1
DEFAULT_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/myqueue
HEALTH_CHECK_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/health

# Default Queue Names
DEFAULT_QUEUE_NAME=defaultQueue
HEALTH_CHECK_QUEUE_NAME=healthCheckQueue
```

### Programmatic Configuration

You can also configure providers programmatically:

```typescript
const queueProvider = new QueueProvider(
  {
    queueConfig: {
      defaultProvider: 'bullmq',
      providers: {
        'bullmq': {
          enabled: true,
          config: {
            redis: {
              host: 'redis.example.com',
              port: 6379,
              password: 'secret'
            }
          }
        },
        'in-memory': {
          enabled: true,
          config: {}
        }
      }
    }
  },
  context
);
```

## Usage Examples

### Basic Usage

```typescript
// Get the queue provider from the service registry
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');

// Get the default queue
const queue = queueProvider.getDefaultProvider();

// Create a message
const message: EventEnvelope = {
  header: {
    id: 'msg-001',
    receivedTimestamp: Date.now(),
    provider: queue.provider
  },
  body: {
    validationResults: []
  }
};

// Enqueue the message
const messageId = await queue.enqueue(message);

// Receive messages
const messages = await queue.receiveMessages({ max: 10 });

// Process and delete
for (const msg of messages) {
  // Process message...
  await queue.deleteMessage(msg.header.id);
}
```

### Multiple Providers with Fallback

```typescript
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');

async function enqueueWithFallback(message: EventEnvelope) {
  // Try BullMQ first
  let queue = queueProvider.getProvider('bullmq');
  
  try {
    if (queue) {
      return await queue.enqueue(message);
    }
  } catch (error) {
    console.warn('BullMQ failed, falling back to in-memory');
  }
  
  // Fallback to in-memory
  queue = queueProvider.getProvider('in-memory');
  if (queue) {
    return await queue.enqueue(message);
  }
  
  throw new Error('All queue providers failed');
}
```

### Environment-Based Selection

```typescript
// Automatically uses the provider configured in REACTORY_QUEUE_PROVIDER
const queue = queueProvider.getDefaultProvider();

// Development: uses in-memory
// Staging: uses BullMQ with Redis
// Production: uses AWS SQS
await queue.enqueue(message);
```

## Testing

### Unit Testing with In-Memory Queue

```typescript
import { QueueProvider } from './QueueProvider';
import { InMemoryQueueService } from './InMemoryQueueService';

describe('My Service', () => {
  let queueProvider: QueueProvider;
  let context: Reactory.Server.IReactoryContext;

  beforeEach(() => {
    // Use in-memory queue for testing
    queueProvider = new QueueProvider(
      {
        queueConfig: {
          defaultProvider: 'in-memory',
          providers: {
            'in-memory': { enabled: true, config: {} }
          }
        }
      },
      context
    );
  });

  test('should enqueue message', async () => {
    const queue = queueProvider.getDefaultProvider();
    const message = createTestMessage();
    
    const messageId = await queue.enqueue(message);
    expect(messageId).toBeDefined();
    
    const count = await queue.count();
    expect(count).toBe(1);
  });
});
```

## Best Practices

1. **Use the Default Provider**: Always use `getDefaultProvider()` for standard operations to allow configuration-based switching.

2. **Implement Fallbacks**: For critical operations, implement fallback logic to alternative providers.

3. **Environment-Specific Configuration**: 
   - Development: `in-memory`
   - Staging: `bullmq`
   - Production: `aws-sqs` or `bullmq`

4. **Error Handling**: Always wrap queue operations in try-catch blocks and handle failures gracefully.

5. **Message Validation**: Validate message envelopes before enqueueing to catch errors early.

6. **Resource Cleanup**: Properly close/delete queues when they're no longer needed.

7. **Health Checks**: All providers implement startup health checks. Monitor these during deployment.

## Creating Custom Providers

To create a custom queue provider:

1. Implement the `QueueServiceType` interface
2. Add the `@service` decorator
3. Register it with the QueueProvider

```typescript
import { service } from "@reactory/server-core/application/decorators";
import { QueueServiceType, EventEnvelope } from './types';

@service({
  id: 'myapp.CustomQueueService@1.0.0',
  nameSpace: 'myapp',
  name: 'CustomQueueService',
  version: '1.0.0',
  description: "My custom queue implementation",
  serviceType: 'data',
  roles: ['USER']
})
export class CustomQueueService implements QueueServiceType {
  provider: string = 'custom';
  
  // Implement all required methods...
  async enqueue(message: EventEnvelope): Promise<string> {
    // Your implementation
  }
  
  // ... other methods
}

// Register with QueueProvider
queueProvider.registerProvider('custom', new CustomQueueService(props, context));
```

## Troubleshooting

### Provider Not Found
```
Queue provider 'bullmq' not found
```
**Solution**: Check that the provider is enabled in configuration and all dependencies are installed.

### Redis Connection Failed
```
Error connecting to Redis
```
**Solution**: Verify Redis is running and connection details are correct. Check `REACTORY_REDIS_HOST`, `REACTORY_REDIS_PORT`, and `REACTORY_REDIS_PASSWORD`.

### AWS SQS Authentication Error
```
AWS credentials not configured
```
**Solution**: Ensure AWS credentials are properly configured via environment variables or IAM role.

## Performance Considerations

- **In-Memory**: Fastest, but limited by available RAM
- **BullMQ**: Good balance of speed and reliability
- **AWS SQS**: Best for scalability, some latency due to network calls

## See Also

- [QueueProvider.example.ts](./QueueProvider.example.ts) - Comprehensive usage examples
- [types.ts](./types.ts) - TypeScript type definitions
- [BullMQ Documentation](https://docs.bullmq.io/)
- [AWS SQS Documentation](https://docs.aws.amazon.com/sqs/)

