import Reactory from "@reactory/reactory-core";
import { Worker, Queue } from 'bullmq';
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
    this.queue = new Queue(DEFAULT_QUEUE_NAME);
    this.healthCheckQueue = new Queue(HEALTH_CHECK_QUEUE_NAME);
  }
  
  async enqueue(message: EventEnvelope, options?: IEnqueueOptions): Promise<string> {
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
    const jobs = await this.queue.getJobs(['active', 'waiting'], 0, options?.maxMessages ?? 10);
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

  async onStartup(context: Reactory.Server.IReactoryContext): Promise<void> {
    this.context = context;

    // Send a health check message to verify queue functionality
    await this.healthCheckQueue.add('healthCheck', { message: 'Health check message' });

    // Process the health check message
    const healthCheckWorker = new Worker(HEALTH_CHECK_QUEUE_NAME, async job => {
      console.log('Processing health check message:', job.data.message);
    });

    healthCheckWorker.on('completed', () => {
      console.log('Health check message processed successfully');
      healthCheckWorker.close();
    });

    healthCheckWorker.on('failed', (job, err) => {
      console.error(`Health check job ${job.id} failed:`, err);
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
