/**
 * QueueProvider Usage Examples
 * 
 * This file demonstrates how to use the QueueProvider to manage
 * multiple queue service implementations in your Reactory application.
 */

import { QueueProvider } from './QueueProvider';
import { EventEnvelope } from './types';

/**
 * Example 1: Basic Usage - Getting the Default Provider
 * 
 * The simplest way to use the QueueProvider is to get the default provider
 * and use it directly.
 */
export async function example1_BasicUsage(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  // Get the default provider (configured via environment or props)
  const queueService = queueProvider.getDefaultProvider();
  
  if (!queueService) {
    throw new Error('No default queue provider available');
  }

  // Create a message envelope
  const message: EventEnvelope = {
    header: {
      id: 'msg-001',
      receivedTimestamp: Date.now(),
      provider: 'default'
    },
    body: {
      validationResults: ['Message validated successfully']
    }
  };

  // Enqueue the message
  const messageId = await queueService.enqueue(message);
  context.log(`Message enqueued with ID: ${messageId}`);

  // Receive messages
  const messages = await queueService.receiveMessages({ max: 10 });
  context.log(`Received ${messages.length} messages`);
}

/**
 * Example 2: Using a Specific Provider
 * 
 * You can explicitly request a specific queue provider by key.
 */
export async function example2_SpecificProvider(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  // Get the in-memory provider specifically
  const inMemoryQueue = queueProvider.getProvider('in-memory');
  
  if (!inMemoryQueue) {
    context.warn('In-memory queue provider not available');
    return;
  }

  // Use the in-memory queue
  const message: EventEnvelope = {
    header: {
      id: 'test-msg-001',
      receivedTimestamp: Date.now(),
      provider: 'in-memory'
    },
    body: {
      validationResults: []
    }
  };

  await inMemoryQueue.enqueue(message, { queueId: 'test-queue' });
  const count = await inMemoryQueue.count('test-queue');
  context.log(`Queue has ${count} messages`);
}

/**
 * Example 3: Working with Multiple Providers
 * 
 * You might want to use different providers for different purposes
 * (e.g., Redis for production, in-memory for testing).
 */
export async function example3_MultipleProviders(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  // Check which providers are available
  const availableProviders = queueProvider.getAvailableProviders();
  context.log(`Available providers: ${availableProviders.join(', ')}`);

  // Use BullMQ for high-throughput production messages
  if (queueProvider.hasProvider('bullmq')) {
    const bullQueue = queueProvider.getProvider('bullmq');
    const message: EventEnvelope = {
      header: {
        id: 'prod-msg-001',
        receivedTimestamp: Date.now(),
        provider: 'bullmq'
      },
      body: {}
    };
    await bullQueue?.enqueue(message);
    context.log('Message sent to BullMQ');
  }

  // Use in-memory for quick testing/development
  if (queueProvider.hasProvider('in-memory')) {
    const memQueue = queueProvider.getProvider('in-memory');
    const testMessage: EventEnvelope = {
      header: {
        id: 'test-msg-002',
        receivedTimestamp: Date.now(),
        provider: 'in-memory'
      },
      body: {}
    };
    await memQueue?.enqueue(testMessage);
    context.log('Test message sent to in-memory queue');
  }
}

/**
 * Example 4: Changing the Default Provider at Runtime
 * 
 * You can dynamically change which provider is used as the default.
 */
export async function example4_ChangeDefaultProvider(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  // Check current default
  const currentDefault = queueProvider.getDefaultProvider();
  context.log(`Current default provider: ${currentDefault?.provider}`);

  // Change to a different provider
  if (queueProvider.hasProvider('bullmq')) {
    queueProvider.setDefaultProvider('bullmq');
    context.log('Default provider changed to BullMQ');
  }

  // Now getDefaultProvider() will return BullMQ
  const newDefault = queueProvider.getDefaultProvider();
  context.log(`New default provider: ${newDefault?.provider}`);
}

/**
 * Example 5: Queue Management Operations
 * 
 * Create, delete, and manage queues across different providers.
 */
export async function example5_QueueManagement(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  const queueService = queueProvider.getProvider('in-memory');
  
  if (!queueService) {
    throw new Error('Queue service not available');
  }

  // Create a new queue
  const created = await queueService.createQueue('my-custom-queue');
  context.log(`Queue created: ${created}`);

  // Enqueue some messages
  for (let i = 0; i < 5; i++) {
    const message: EventEnvelope = {
      header: {
        id: `msg-${i}`,
        receivedTimestamp: Date.now(),
        provider: 'in-memory'
      },
      body: {}
    };
    await queueService.enqueue(message, { queueId: 'my-custom-queue' });
  }

  // Check message count
  const count = await queueService.count('my-custom-queue');
  context.log(`Queue contains ${count} messages`);

  // Delete the queue
  const deleted = await queueService.deleteQueue('my-custom-queue');
  context.log(`Queue deleted: ${deleted}`);
}

/**
 * Example 6: Configuration-Based Provider Selection
 * 
 * Use environment-based configuration to automatically select providers.
 */
export async function example6_ConfigurationBasedSelection(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  // The QueueProvider automatically loads configuration from:
  // 1. Environment variables (REACTORY_QUEUE_PROVIDER, REACTORY_QUEUE_BULLMQ_ENABLED, etc.)
  // 2. Props passed to constructor
  // 3. Default configuration

  // Get the provider that was configured as default
  const defaultProvider = queueProvider.getDefaultProvider();
  
  if (!defaultProvider) {
    context.error('No default provider configured');
    return;
  }

  context.log(`Using configured provider: ${defaultProvider.provider}`);
  
  // Use it for your operations
  const message: EventEnvelope = {
    header: {
      id: 'config-msg-001',
      receivedTimestamp: Date.now(),
      provider: defaultProvider.provider
    },
    body: {}
  };

  await defaultProvider.enqueue(message);
}

/**
 * Example 7: Error Handling and Fallback
 * 
 * Implement robust error handling with fallback to alternative providers.
 */
export async function example7_ErrorHandlingWithFallback(
  queueProvider: QueueProvider,
  context: Reactory.Server.IReactoryContext
) {
  const message: EventEnvelope = {
    header: {
      id: 'fallback-msg-001',
      receivedTimestamp: Date.now(),
      provider: 'primary'
    },
    body: {}
  };

  // Try primary provider (e.g., BullMQ)
  const primaryProvider = queueProvider.getProvider('bullmq');
  
  try {
    if (primaryProvider) {
      await primaryProvider.enqueue(message);
      context.log('Message sent via primary provider');
    } else {
      throw new Error('Primary provider not available');
    }
  } catch (error) {
    context.warn('Primary provider failed, falling back to in-memory', error);
    
    // Fallback to in-memory queue
    const fallbackProvider = queueProvider.getProvider('in-memory');
    if (fallbackProvider) {
      await fallbackProvider.enqueue(message);
      context.log('Message sent via fallback provider');
    } else {
      context.error('All providers failed', error);
      throw error;
    }
  }
}

/**
 * Example 8: Using QueueProvider in a Service
 * 
 * Typical integration pattern in a Reactory service.
 */
export class MyCustomService implements Reactory.Service.IReactoryDefaultService {
  nameSpace: string = 'myapp';
  name: string = 'MyCustomService';
  version: string = '1.0.0';
  
  context: Reactory.Server.IReactoryContext;
  queueProvider: QueueProvider;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.context = context;
    
    // Get the QueueProvider from the service registry
    this.queueProvider = context.getService('reactory.QueueProvider@1.0.0') as QueueProvider;
  }

  async processData(data: any): Promise<void> {
    // Use the queue provider in your service methods
    const queue = this.queueProvider.getDefaultProvider();
    
    if (!queue) {
      throw new Error('Queue service not available');
    }

    const message: EventEnvelope = {
      header: {
        id: `${Date.now()}-${Math.random()}`,
        receivedTimestamp: Date.now(),
        provider: queue.provider
      },
      body: {
        validationResults: []
      }
    };

    await queue.enqueue(message);
    this.context.log('Data processing message enqueued');
  }

  getExecutionContext(): Reactory.Server.IReactoryContext {
    return this.context;
  }

  setExecutionContext(context: Reactory.Server.IReactoryContext): void {
    this.context = context;
  }
}

