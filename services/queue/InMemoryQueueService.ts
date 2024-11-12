import Reactory from "@reactory/reactory-core";
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
  id: 'reactory.InMemoryQueueService@1.0.0',
  nameSpace: 'reactory',
  name: 'InMemoryQueueService',
  version: '1.0.0',
  description: "A queue wrapper service that provides a light abstraction for in-memory queue services",
  dependencies: [],
  serviceType: 'data',
  roles: ['USER']
})
export class InMemoryQueueService implements QueueServiceType {
  
  description?: string;
  tags?: string[];
  nameSpace: string;
  name: string;
  version: string;

  props: any;
  context: Reactory.Server.IReactoryContext;

  provider: string = 'in-memory';
  queue: Map<string, EventEnvelope[]>;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;
    this.queue = new Map();
    this.queue.set(DEFAULT_QUEUE_NAME, []);
    this.queue.set(HEALTH_CHECK_QUEUE_NAME, []);
  }
  
  async enqueue(message: EventEnvelope, options?: IEnqueueOptions): Promise<string> {
    const queueName = options?.queueId || DEFAULT_QUEUE_NAME;
    const queue = this.queue.get(queueName);
    if (queue) {
      queue.push(message);
      return `${queueName}-${queue.length - 1}`; // Simulated message ID
    }
    throw new Error(`Queue ${queueName} does not exist.`);
  }

  async dequeue(queueId: string = DEFAULT_QUEUE_NAME): Promise<EventEnvelope> {
    const queue = this.queue.get(queueId);
    if (queue && queue.length > 0) {
      return queue.shift() as EventEnvelope;
    }
    throw new Error(`No messages available in queue ${queueId}.`);
  }

  async deleteMessage(id: string, options?: DeleteMessageOptions): Promise<void> {
    const [queueId, indexStr] = id.split('-');
    const index = parseInt(indexStr);
    const queue = this.queue.get(queueId);
    if (queue && queue[index]) {
      queue.splice(index, 1);
    }
  }

  async deleteMessages(ids: string[], options?: DeleteMessageOptions): Promise<void> {
    await Promise.all(ids.map(id => this.deleteMessage(id, options)));
  }

  async receiveMessages(options?: ReceiveMessageOptions): Promise<EventEnvelope[]> {
    const queueName = options?.queueId || DEFAULT_QUEUE_NAME;
    const queue = this.queue.get(queueName);
    if (!queue) throw new Error(`Queue ${queueName} does not exist.`);
    const maxMessages = options?.maxMessages ?? queue.length;
    return queue.slice(0, maxMessages);
  }

  async count(queueId: string = DEFAULT_QUEUE_NAME): Promise<number> {
    const queue = this.queue.get(queueId);
    return queue ? queue.length : 0;
  }

  async createQueue(queueId: string): Promise<boolean> {
    if (!this.queue.has(queueId)) {
      this.queue.set(queueId, []);
      return true;
    }
    return false;
  }

  async deleteQueue(queueId: string): Promise<boolean> {
    return this.queue.delete(queueId);
  }

  async onStartup(context: Reactory.Server.IReactoryContext): Promise<void> {
    this.context = context;

    // Enqueue a health check message
    const healthCheckMessage: EventEnvelope = { message: 'Health check message', timestamp: Date.now() };
    await this.enqueue(healthCheckMessage, { queueId: HEALTH_CHECK_QUEUE_NAME });

    console.log('Health check message enqueued to in-memory queue.');

    // Process the health check message
    const healthCheckQueue = this.queue.get(HEALTH_CHECK_QUEUE_NAME);
    if (healthCheckQueue && healthCheckQueue.length > 0) {
      const message = healthCheckQueue.shift();
      console.log('Processing health check message:', message);
    }
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
