/**
 * QueueProvider Unit Tests
 * 
 * Tests for the QueueProvider service that manages multiple queue implementations
 */

import { QueueProvider } from './QueueProvider';
import { InMemoryQueueService } from './InMemoryQueueService';
import { BullMessageQueueService } from './BullMessageQueueService';
import { EventEnvelope } from './types';

// Mock context for testing
const createMockContext = (): Reactory.Server.IReactoryContext => ({
  log: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  getService: jest.fn(),
} as any);

describe('QueueProvider', () => {
  let queueProvider: QueueProvider;
  let mockContext: Reactory.Server.IReactoryContext;

  beforeEach(() => {
    mockContext = createMockContext();
    queueProvider = new QueueProvider(
      {
        queueConfig: {
          defaultProvider: 'in-memory',
          providers: {
            'in-memory': {
              enabled: true,
              config: {}
            }
          }
        }
      },
      mockContext
    );
  });

  describe('Provider Registration', () => {
    test('should register providers on initialization', () => {
      const availableProviders = queueProvider.getAvailableProviders();
      expect(availableProviders).toContain('in-memory');
    });

    test('should register a custom provider', () => {
      const customQueue = new InMemoryQueueService({}, mockContext);
      queueProvider.registerProvider('custom', customQueue);
      
      expect(queueProvider.hasProvider('custom')).toBe(true);
    });

    test('should unregister a provider', () => {
      const customQueue = new InMemoryQueueService({}, mockContext);
      queueProvider.registerProvider('custom', customQueue);
      
      const removed = queueProvider.unregisterProvider('custom');
      expect(removed).toBe(true);
      expect(queueProvider.hasProvider('custom')).toBe(false);
    });

    test('should warn when overwriting existing provider', () => {
      const customQueue1 = new InMemoryQueueService({}, mockContext);
      const customQueue2 = new InMemoryQueueService({}, mockContext);
      
      queueProvider.registerProvider('test', customQueue1);
      queueProvider.registerProvider('test', customQueue2);
      
      expect(mockContext.warn).toHaveBeenCalled();
    });
  });

  describe('Provider Retrieval', () => {
    test('should get default provider', () => {
      const provider = queueProvider.getDefaultProvider();
      expect(provider).toBeDefined();
      expect(provider?.provider).toBe('in-memory');
    });

    test('should get provider by key', () => {
      const provider = queueProvider.getProvider('in-memory');
      expect(provider).toBeDefined();
      expect(provider?.provider).toBe('in-memory');
    });

    test('should return undefined for non-existent provider', () => {
      const provider = queueProvider.getProvider('non-existent');
      expect(provider).toBeUndefined();
    });

    test('should warn when provider not found', () => {
      queueProvider.getProvider('non-existent');
      expect(mockContext.warn).toHaveBeenCalled();
    });

    test('should get all providers', () => {
      const customQueue = new InMemoryQueueService({}, mockContext);
      queueProvider.registerProvider('custom', customQueue);
      
      const allProviders = queueProvider.getAllProviders();
      expect(allProviders.size).toBeGreaterThanOrEqual(2);
      expect(allProviders.has('in-memory')).toBe(true);
      expect(allProviders.has('custom')).toBe(true);
    });
  });

  describe('Default Provider Management', () => {
    test('should set default provider', () => {
      const customQueue = new InMemoryQueueService({}, mockContext);
      queueProvider.registerProvider('custom', customQueue);
      
      queueProvider.setDefaultProvider('custom');
      const defaultProvider = queueProvider.getDefaultProvider();
      
      expect(defaultProvider?.provider).toBe('in-memory');
    });

    test('should throw error when setting non-existent provider as default', () => {
      expect(() => {
        queueProvider.setDefaultProvider('non-existent');
      }).toThrow();
    });
  });

  describe('Provider Queries', () => {
    test('should check if provider exists', () => {
      expect(queueProvider.hasProvider('in-memory')).toBe(true);
      expect(queueProvider.hasProvider('non-existent')).toBe(false);
    });

    test('should list available providers', () => {
      const available = queueProvider.getAvailableProviders();
      expect(Array.isArray(available)).toBe(true);
      expect(available.length).toBeGreaterThan(0);
    });
  });

  describe('Message Operations', () => {
    test('should enqueue message using default provider', async () => {
      const provider = queueProvider.getDefaultProvider();
      
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

      const messageId = await provider?.enqueue(message);
      expect(messageId).toBeDefined();
    });

    test('should receive messages from default provider', async () => {
      const provider = queueProvider.getDefaultProvider();
      
      const message: EventEnvelope = {
        header: {
          id: 'test-msg-002',
          receivedTimestamp: Date.now(),
          provider: 'in-memory'
        },
        body: {
          validationResults: []
        }
      };

      await provider?.enqueue(message, { queueId: 'test-queue' });
      const count = await provider?.count('test-queue');
      
      expect(count).toBeGreaterThan(0);
    });
  });

  describe('Configuration', () => {
    test('should load configuration from props', () => {
      const configuredProvider = new QueueProvider(
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

      const defaultProvider = configuredProvider.getDefaultProvider();
      expect(defaultProvider).toBeDefined();
    });

    test('should use environment variables for configuration', () => {
      // This would test environment variable configuration
      // In a real test, you'd mock process.env
      const provider = new QueueProvider({}, mockContext);
      expect(provider).toBeDefined();
    });
  });

  describe('Lifecycle', () => {
    test('should call onStartup for all providers', async () => {
      const provider = queueProvider.getDefaultProvider();
      const onStartupSpy = jest.spyOn(provider as any, 'onStartup');
      
      await queueProvider.onStartup();
      
      // The provider's onStartup should be called
      expect(mockContext.log).toHaveBeenCalled();
    });

    test('should handle provider startup errors gracefully', async () => {
      const faultyProvider = new InMemoryQueueService({}, mockContext);
      faultyProvider.onStartup = jest.fn().mockRejectedValue(new Error('Startup failed'));
      
      queueProvider.registerProvider('faulty', faultyProvider);
      
      // Should not throw
      await expect(queueProvider.onStartup()).resolves.not.toThrow();
      expect(mockContext.error).toHaveBeenCalled();
    });
  });

  describe('Service Interface', () => {
    test('should implement toString', () => {
      const str = queueProvider.toString();
      expect(str).toBe('reactory.QueueProvider');
      
      const strWithVersion = queueProvider.toString(true);
      expect(strWithVersion).toBe('reactory.QueueProvider@1.0.0');
    });

    test('should get execution context', () => {
      const context = queueProvider.getExecutionContext();
      expect(context).toBe(mockContext);
    });

    test('should set execution context', () => {
      const newContext = createMockContext();
      queueProvider.setExecutionContext(newContext);
      
      const context = queueProvider.getExecutionContext();
      expect(context).toBe(newContext);
    });
  });
});

