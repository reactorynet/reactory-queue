import Reactory from "@reactorynet/reactory-core";
import { service } from "@reactory/server-core/application/decorators";
import { QueueServiceType } from './types';
import { BullMessageQueueService } from './BullMessageQueueService';
import { InMemoryQueueService } from './InMemoryQueueService';
import { AWSSQSQueueService } from './AWSSQSService';

export interface IQueueProviderConfig {
  /**
   * The default provider key to use when no key is specified
   */
  defaultProvider?: string;
  /**
   * Provider-specific configurations
   */
  providers?: {
    [key: string]: {
      enabled: boolean;
      config?: any;
    };
  };
}

/**
 * QueueProvider - A central service that manages multiple queue service providers
 * and provides a unified interface for accessing them.
 * 
 * Supports multiple queue backends:
 * - 'bullmq' - BullMQ with Redis
 * - 'in-memory' - In-memory queue (for testing/development)
 * - 'aws-sqs' - AWS SQS
 */
@service({
  id: 'reactory.QueueProvider@1.0.0',
  nameSpace: 'reactory',
  name: 'QueueProvider',
  version: '1.0.0',
  description: "A provider service that manages and provides access to multiple queue service implementations",
  dependencies: [],
  serviceType: 'data',
  roles: ['SYSTEM']
})
export class QueueProvider implements Reactory.Service.IReactoryDefaultService {
  
  description?: string;
  tags?: string[];
  nameSpace: string;
  name: string;
  version: string;

  props: any;
  context: Reactory.Server.IReactoryContext;

  private providers: Map<string, QueueServiceType>;
  private defaultProviderKey: string;
  private config: IQueueProviderConfig;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;
    this.providers = new Map();
    
    // Load configuration from props or environment
    this.config = this.loadConfiguration(props);
    this.defaultProviderKey = this.config.defaultProvider || process.env.REACTORY_QUEUE_PROVIDER || 'in-memory';
    
    // Initialize providers
    this.initializeProviders();
  }

  /**
   * Load configuration from props, context, or environment
   */
  private loadConfiguration(props: any): IQueueProviderConfig {
    const defaultConfig: IQueueProviderConfig = {
      defaultProvider: 'in-memory',
      providers: {
        'bullmq': {
          enabled: process.env.REACTORY_QUEUE_BULLMQ_ENABLED === 'true',
          config: {
            redis: {
              host: process.env.REACTORY_REDIS_HOST || 'localhost',
              port: parseInt(process.env.REACTORY_REDIS_PORT || '6379', 10),
              password: process.env.REACTORY_REDIS_PASSWORD || 'reactory'
            }
          }
        },
        'in-memory': {
          enabled: true,
          config: {}
        },
        'aws-sqs': {
          enabled: process.env.REACTORY_QUEUE_AWS_SQS_ENABLED === 'true',
          config: {
            region: process.env.AWS_REGION || 'us-east-1',
            queueUrl: process.env.DEFAULT_SQS_QUEUE_URL
          }
        }
      }
    };

    // Merge with props config if provided
    if (props?.queueConfig) {
      return {
        ...defaultConfig,
        ...props.queueConfig,
        providers: {
          ...defaultConfig.providers,
          ...props.queueConfig.providers
        }
      };
    }

    return defaultConfig;
  }

  /**
   * Initialize all enabled queue providers
   */
  private initializeProviders(): void {
    const { context } = this;
    const providerConfig = this.config.providers || {};

    // Register BullMQ provider if enabled
    if (providerConfig['bullmq']?.enabled) {
      try {
        const bullService = new BullMessageQueueService(
          providerConfig['bullmq'].config,
          context
        );
        this.registerProvider('bullmq', bullService);
        context.log('BullMQ queue provider registered', 'QueueProvider.initializeProviders');
      } catch (error) {
        context.error('Failed to initialize BullMQ provider', error, 'QueueProvider.initializeProviders');
      }
    }

    // Register In-Memory provider (always enabled for fallback)
    if (providerConfig['in-memory']?.enabled !== false) {
      try {
        const inMemoryService = new InMemoryQueueService(
          providerConfig['in-memory']?.config || {},
          context
        );
        this.registerProvider('in-memory', inMemoryService);
        context.log('In-Memory queue provider registered', 'QueueProvider.initializeProviders');
      } catch (error) {
        context.error('Failed to initialize In-Memory provider', error, 'QueueProvider.initializeProviders');
      }
    }

    // Register AWS SQS provider if enabled
    if (providerConfig['aws-sqs']?.enabled) {
      try {
        const sqsService = new AWSSQSQueueService(
          providerConfig['aws-sqs'].config,
          context
        );
        this.registerProvider('aws-sqs', sqsService);
        context.log('AWS SQS queue provider registered', 'QueueProvider.initializeProviders');
      } catch (error) {
        context.error('Failed to initialize AWS SQS provider', error, 'QueueProvider.initializeProviders');
      }
    }
  }

  /**
   * Register a queue provider with a specific key
   * @param key - The unique identifier for this provider
   * @param provider - The queue service instance
   */
  registerProvider(key: string, provider: QueueServiceType): void {
    if (this.providers.has(key)) {
      this.context.warn(`Provider with key '${key}' already exists. Overwriting.`, 'QueueProvider.registerProvider');
    }
    this.providers.set(key, provider);
  }

  /**
   * Unregister a queue provider
   * @param key - The unique identifier of the provider to remove
   */
  unregisterProvider(key: string): boolean {
    return this.providers.delete(key);
  }

  /**
   * Get a queue provider by key
   * @param key - The provider key (e.g., 'bullmq', 'in-memory', 'aws-sqs')
   * @returns The queue service instance or undefined if not found
   */
  getProvider(key?: string): QueueServiceType | undefined {
    const providerKey = key || this.defaultProviderKey;
    const provider = this.providers.get(providerKey);
    
    if (!provider) {
      this.context.warn(
        `Queue provider '${providerKey}' not found. Available providers: ${this.getAvailableProviders().join(', ')}`,
        'QueueProvider.getProvider'
      );
    }
    
    return provider;
  }

  /**
   * Get the default queue provider
   * @returns The default queue service instance
   */
  getDefaultProvider(): QueueServiceType | undefined {
    return this.getProvider(this.defaultProviderKey);
  }

  /**
   * Set the default provider key
   * @param key - The provider key to set as default
   */
  setDefaultProvider(key: string): void {
    if (this.providers.has(key)) {
      this.defaultProviderKey = key;
      this.context.log(`Default queue provider set to '${key}'`, 'QueueProvider.setDefaultProvider');
    } else {
      throw new Error(`Cannot set default provider to '${key}': provider not registered`);
    }
  }

  /**
   * Get a list of all registered provider keys
   * @returns Array of provider keys
   */
  getAvailableProviders(): string[] {
    return Array.from(this.providers.keys());
  }

  /**
   * Check if a provider is registered
   * @param key - The provider key to check
   */
  hasProvider(key: string): boolean {
    return this.providers.has(key);
  }

  /**
   * Get all registered providers
   * @returns Map of all providers
   */
  getAllProviders(): Map<string, QueueServiceType> {
    return new Map(this.providers);
  }

  /**
   * Service lifecycle hook - called when the service starts
   */
  async onStartup(): Promise<void> {
    const { context } = this;
    context.log('QueueProvider starting up', 'QueueProvider.onStartup');
    
    // Initialize all registered providers
    const startupPromises = Array.from(this.providers.entries()).map(async ([key, provider]) => {
      try {
        if (typeof provider.onStartup === 'function') {
          await provider.onStartup(context);
          context.log(`Provider '${key}' started successfully`, 'QueueProvider.onStartup');
        }
      } catch (error) {
        context.error(`Failed to start provider '${key}'`, error, 'QueueProvider.onStartup');
      }
    });

    await Promise.all(startupPromises);
    
    context.log(
      `QueueProvider initialized with ${this.providers.size} provider(s): ${this.getAvailableProviders().join(', ')}`,
      'QueueProvider.onStartup'
    );
    context.log(`Default provider: ${this.defaultProviderKey}`, 'QueueProvider.onStartup');
  }

  toString(includeVersion?: boolean): string {
    return `${this.nameSpace}.${this.name}${includeVersion ? '@' + this.version : ''}`;
  }

  getExecutionContext(): Reactory.Server.IReactoryContext {
    return this.context;
  }

  setExecutionContext(context: Reactory.Server.IReactoryContext): void {
    this.context = context;
  }
}