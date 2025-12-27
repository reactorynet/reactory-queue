# Queue Services Migration Guide

This guide helps you migrate from using individual queue service implementations directly to using the new `QueueProvider` abstraction.

## What Changed?

Previously, you had to instantiate and manage individual queue services directly:

```typescript
// Old approach
const inMemoryQueue = new InMemoryQueueService(props, context);
await inMemoryQueue.enqueue(message);
```

Now, you use the `QueueProvider` to access any queue service through a unified interface:

```typescript
// New approach
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');
const queue = queueProvider.getDefaultProvider();
await queue.enqueue(message);
```

## Benefits of Migration

1. **Configuration-Based Selection**: Switch between queue backends via environment variables
2. **Easier Testing**: Use in-memory queues for testing without code changes
3. **Fallback Support**: Implement fallback logic across multiple providers
4. **Centralized Management**: All queue services registered in one place
5. **Runtime Switching**: Change queue providers at runtime if needed

## Migration Steps

### Step 1: Update Imports

**Before:**
```typescript
import { InMemoryQueueService } from './services/queue/InMemoryQueueService';
import { BullMessageQueueService } from './services/queue/BullMessageQueueService';
```

**After:**
```typescript
import { QueueProvider } from './services/queue/QueueProvider';
// Or use the barrel export:
import { QueueProvider } from './services/queue';
```

### Step 2: Access via Service Registry

**Before:**
```typescript
class MyService {
  queueService: InMemoryQueueService;
  
  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.queueService = new InMemoryQueueService(props, context);
  }
  
  async processData(data: any) {
    await this.queueService.enqueue(message);
  }
}
```

**After:**
```typescript
class MyService {
  queueProvider: QueueProvider;
  
  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.queueProvider = context.getService('reactory.QueueProvider@1.0.0') as QueueProvider;
  }
  
  async processData(data: any) {
    const queue = this.queueProvider.getDefaultProvider();
    if (queue) {
      await queue.enqueue(message);
    }
  }
}
```

### Step 3: Update Configuration

Add environment variables to control which queue provider is used:

```bash
# .env file

# Set the default queue provider
REACTORY_QUEUE_PROVIDER=in-memory  # or 'bullmq', 'aws-sqs'

# Enable/disable specific providers
REACTORY_QUEUE_BULLMQ_ENABLED=true
REACTORY_QUEUE_AWS_SQS_ENABLED=false

# Provider-specific configuration
REACTORY_REDIS_HOST=localhost
REACTORY_REDIS_PORT=6379
REACTORY_REDIS_PASSWORD=reactory

AWS_REGION=us-east-1
DEFAULT_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/myqueue
```

### Step 4: Update Tests

**Before:**
```typescript
describe('MyService', () => {
  let service: MyService;
  let mockContext: any;
  
  beforeEach(() => {
    mockContext = createMockContext();
    // Had to manually instantiate the queue service
    const queueService = new InMemoryQueueService({}, mockContext);
    service = new MyService({ queueService }, mockContext);
  });
  
  test('should process data', async () => {
    await service.processData(testData);
    // assertions...
  });
});
```

**After:**
```typescript
describe('MyService', () => {
  let service: MyService;
  let mockContext: any;
  let queueProvider: QueueProvider;
  
  beforeEach(() => {
    mockContext = createMockContext();
    
    // QueueProvider automatically sets up in-memory queue
    queueProvider = new QueueProvider(
      {
        queueConfig: {
          defaultProvider: 'in-memory',
          providers: {
            'in-memory': { enabled: true, config: {} }
          }
        }
      },
      mockContext
    );
    
    // Mock the service registry to return our queue provider
    mockContext.getService = jest.fn().mockReturnValue(queueProvider);
    
    service = new MyService({}, mockContext);
  });
  
  test('should process data', async () => {
    await service.processData(testData);
    // assertions...
  });
});
```

## Migration Patterns

### Pattern 1: Direct Replacement

If you're using a single queue service throughout your application:

**Before:**
```typescript
const queue = new InMemoryQueueService(props, context);
await queue.enqueue(message);
const messages = await queue.receiveMessages();
```

**After:**
```typescript
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');
const queue = queueProvider.getDefaultProvider();
await queue?.enqueue(message);
const messages = await queue?.receiveMessages();
```

### Pattern 2: Specific Provider Selection

If you need a specific queue implementation:

**Before:**
```typescript
const redisQueue = new BullMessageQueueService(redisConfig, context);
await redisQueue.enqueue(message);
```

**After:**
```typescript
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');
const redisQueue = queueProvider.getProvider('bullmq');
if (redisQueue) {
  await redisQueue.enqueue(message);
}
```

### Pattern 3: Fallback Logic

Add robustness with fallback providers:

**Before:**
```typescript
// No fallback - fails if BullMQ is down
const queue = new BullMessageQueueService(config, context);
await queue.enqueue(message);
```

**After:**
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
    context.warn('BullMQ failed, falling back', error);
  }
  
  // Fallback to in-memory
  queue = queueProvider.getProvider('in-memory');
  if (queue) {
    return await queue.enqueue(message);
  }
  
  throw new Error('All queue providers failed');
}

await enqueueWithFallback(message);
```

### Pattern 4: Environment-Based Selection

Let configuration control the queue backend:

**Before:**
```typescript
// Hardcoded to use BullMQ
const queue = new BullMessageQueueService(config, context);
```

**After:**
```typescript
// Automatically uses the configured provider
const queueProvider = context.getService('reactory.QueueProvider@1.0.0');
const queue = queueProvider.getDefaultProvider();

// In development: uses in-memory (REACTORY_QUEUE_PROVIDER=in-memory)
// In production: uses BullMQ (REACTORY_QUEUE_PROVIDER=bullmq)
```

## Common Issues and Solutions

### Issue 1: Provider Not Found

**Error:**
```
Queue provider 'bullmq' not found. Available providers: in-memory
```

**Solution:**
Enable the provider in your configuration:
```bash
REACTORY_QUEUE_BULLMQ_ENABLED=true
```

Or register it manually:
```typescript
const bullQueue = new BullMessageQueueService(config, context);
queueProvider.registerProvider('bullmq', bullQueue);
```

### Issue 2: Default Provider Returns Undefined

**Error:**
```
Cannot read property 'enqueue' of undefined
```

**Solution:**
Check if a provider is available before using it:
```typescript
const queue = queueProvider.getDefaultProvider();
if (!queue) {
  throw new Error('No queue provider available');
}
await queue.enqueue(message);
```

### Issue 3: Wrong Provider Type

**Error:**
```
Expected 'bullmq' but got 'in-memory'
```

**Solution:**
Verify your environment configuration:
```bash
# Check that REACTORY_QUEUE_PROVIDER is set correctly
echo $REACTORY_QUEUE_PROVIDER

# Set it if needed
export REACTORY_QUEUE_PROVIDER=bullmq
```

## Testing Considerations

### Use In-Memory for Unit Tests

Always use the in-memory provider for unit tests:

```typescript
beforeEach(() => {
  const queueProvider = new QueueProvider(
    {
      queueConfig: {
        defaultProvider: 'in-memory',
        providers: {
          'in-memory': { enabled: true, config: {} }
        }
      }
    },
    mockContext
  );
  
  // Inject into services
  mockContext.getService = jest.fn().mockReturnValue(queueProvider);
});
```

### Use BullMQ for Integration Tests

For integration tests, use the actual queue backend:

```typescript
beforeAll(async () => {
  const queueProvider = new QueueProvider(
    {
      queueConfig: {
        defaultProvider: 'bullmq',
        providers: {
          'bullmq': {
            enabled: true,
            config: {
              redis: {
                host: process.env.TEST_REDIS_HOST || 'localhost',
                port: 6379
              }
            }
          }
        }
      }
    },
    context
  );
  
  await queueProvider.onStartup();
});
```

## Backward Compatibility

The individual queue services (BullMessageQueueService, InMemoryQueueService, AWSSQSQueueService) are still available and can be used directly if needed. However, we recommend migrating to the QueueProvider for the benefits listed above.

## Gradual Migration

You can migrate gradually:

1. **Phase 1**: Install QueueProvider alongside existing services
2. **Phase 2**: Migrate new code to use QueueProvider
3. **Phase 3**: Refactor existing code module by module
4. **Phase 4**: Remove direct queue service instantiations

## Need Help?

- See [README.md](./README.md) for full API documentation
- See [QueueProvider.example.ts](./QueueProvider.example.ts) for usage examples
- Check the test files for testing patterns

## Checklist

Use this checklist to track your migration:

- [ ] Update imports to use QueueProvider
- [ ] Replace direct instantiations with getService() calls
- [ ] Update configuration (environment variables)
- [ ] Update tests to use QueueProvider
- [ ] Add null checks for queue provider access
- [ ] Test with different queue backends
- [ ] Update documentation
- [ ] Remove old queue service instantiations
- [ ] Verify in development environment
- [ ] Verify in staging/production environment

