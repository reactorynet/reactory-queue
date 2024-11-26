import Reactory from "@reactory/reactory-core";
import { Worker, Queue, JobsOptions } from 'bullmq';
import { service } from "@reactory/server-core/application/decorators";
import { 
  DeleteMessageOptions, 
  EventEnvelope, 
  QueueServiceType, 
  IEnqueueOptions, 
  ReceiveMessageOptions 
} from './types';

const {
  DEFAULT_QUEUE_NAME = 'defaultQueue',
  HEALTH_CHECK_QUEUE_NAME = 'healthCheckQueue'
} = process.env;

@service({
  id: 'reactory.BullMessageQueueService@1.0.0',
  nameSpace: 'reactory',
  name: 'BullMessageQueueService',
  version: '1.0.0',
  description: "A queue wrapper service that provides a light abstraction for bullmq queue services",
  dependencies: [],
  serviceType: 'data',
  roles: ['USER']
})
export class BullMessageQueueService implements QueueServiceType {
  
  description?: string;
  tags?: string[];
  nameSpace: string;
  name: string;
  version: string;

  props: any;
  context: Reactory.Server.IReactoryContext;

  provider: string = 'bullmq';
  queue: Queue;
  healthCheckQueue: Queue;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;

    // Initialize BullMQ queues with default queue names
    this.queue = new Queue(DEFAULT_QUEUE_NAME, { 
      connection: {
        host: process.env.REACTORY_REDIS_HOST || 'localhost',
        port: parseInt(process.env.REACTORY_REDIS_PORT || '6379', 10),
        password: process.env.REACTORY_REDIS_PASSWORD || 'reactory'
      }
    });

    this.healthCheckQueue = new Queue(HEALTH_CHECK_QUEUE_NAME, {
      connection: {
        host: process.env.REACTORY_REDIS_HOST || 'localhost',
        port: parseInt(process.env.REACTORY_REDIS_PORT || '6379', 10),
        password: process.env.REACTORY_REDIS_PASSWORD || 'reactory'
      }
    });
  }
  
  async enqueue(message: EventEnvelope, options?: JobsOptions): Promise<string> {
    const job = await this.queue.add('enqueueJob', message, options);
    return job.id;
  }

  async dequeue(): Promise<EventEnvelope> {
    const job = await this.queue.getJob('enqueueJob');
    if (job) {
      const message = job.data as EventEnvelope;
      await job.remove();
      return message;
    }
    throw new Error("No message found in queue.");
  }

  async deleteMessage(id: string, options?: DeleteMessageOptions): Promise<void> {
    const job = await this.queue.getJob(id);
    if (job) {
      await job.remove();
    }
  }

  async deleteMessages(ids: string[], options?: DeleteMessageOptions): Promise<void> {
    await Promise.all(ids.map(id => this.deleteMessage(id, options)));
  }

  async receiveMessages(options?: ReceiveMessageOptions): Promise<EventEnvelope[]> {
    const jobs = await this.queue.getJobs(['active', 'waiting'], 0, options?.max ?? 10);
    return jobs.map(job => job.data as EventEnvelope);
  }

  async count(queueId?: string): Promise<number> {
    return await this.queue.count();
  }

  async createQueue(queueId: string): Promise<boolean> {
    this.queue = new Queue(queueId);
    return true;
  }

  async deleteQueue(queueId: string): Promise<boolean> {
    await this.queue.drain();
    await this.queue.close();
    return true;
  }

  async onStartup(): Promise<void> {
    const { context } = this;
    context.log('BullMessageQueueService starting');
    // Send a health check message to verify queue functionality
    await this.healthCheckQueue.add('healthCheck', { message: 'Health check message' });

    // Process the health check message
    const healthCheckWorker = new Worker(HEALTH_CHECK_QUEUE_NAME, async job => {
      context.log('Processing health check message:', job.data.message);
    }, { connection: {
      host: process.env.REACTORY_REDIS_HOST || 'localhost',
      port: parseInt(process.env.REACTORY_REDIS_PORT || '6379', 10),
      password: process.env.REACTORY_REDIS_PASSWORD || 'reactory'
    } });

    healthCheckWorker.on('completed', () => {
      context.log('Health check message processed successfully');
      healthCheckWorker.close();
    });

    healthCheckWorker.on('failed', (job, err) => {
      context.error(`Health check job ${job.id} failed:`, err);
    });
  }
  
  toString?(includeVersion?: boolean): string {
    return `${this.nameSpace}.${this.name}${includeVersion ? '@' + this.version : ''}`;
  }
  
  getExecutionContext(): Reactory.Server.IReactoryContext {
    return this.context;
  }
  setExecutionContext(context: Reactory.Server.IReactoryContext): void {
    this.context = context;
  }
}
