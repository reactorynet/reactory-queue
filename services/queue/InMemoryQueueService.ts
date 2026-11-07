import Reactory from "@reactorynet/reactory-core";
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

  /**
   * Monotonic counter backing delivery handles.
   *
   * The handle used to be `<queueName>-<index>`, and an array index is NOT an
   * identity: every removal shifts the array underneath it. A handle minted
   * before a removal then either points at a DIFFERENT envelope (so the wrong
   * message was deleted — losing work that still had to be sent) or past the end
   * (`queue[index]` undefined, so nothing was deleted and the envelope was
   * re-peeked on every cycle forever).
   *
   * The second case is what produced the endless
   * `Envelope skipped: message is unknown or already terminal` warnings: the
   * envelope for an already-terminal message could never be removed, so every
   * poll re-delivered it, re-claimed nothing, and warned again.
   *
   * A sequence number is stable for the lifetime of the envelope.
   */
  private deliverySequence = 0;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;
    this.queue = new Map();
    this.queue.set(DEFAULT_QUEUE_NAME, []);
    this.queue.set(HEALTH_CHECK_QUEUE_NAME, []);
  }
  
  async enqueue(message: EventEnvelope, options?: IEnqueueOptions): Promise<string> {
    const queueName = options?.queueId || DEFAULT_QUEUE_NAME;
    let queue = this.queue.get(queueName);

    if (!queue) {
      // The provider has no implicit queue creation: without the opt-in flag an
      // unknown queue id is an error. Honouring the flag here is what callers
      // that pass `createIfNotExists: true` rely on.
      if (!options?.createIfNotExists) {
        throw new Error(`Queue ${queueName} does not exist.`);
      }
      queue = [];
      this.queue.set(queueName, queue);
    }

    // Stamp a stable identity on the envelope BEFORE storing it, so a consumer
    // that received the envelope can settle it by that identity without needing
    // the handle returned here. `QueueListener` deletes by `header.id`, and other
    // consumers delete by the returned handle; both must work.
    this.deliverySequence += 1;
    const deliveryId = `${queueName}#${this.deliverySequence}`;
    if (message && message.header) {
      (message.header as any).deliveryId = deliveryId;
    }

    queue.push(message);
    return deliveryId;
  }

  async dequeue(queueId: string = DEFAULT_QUEUE_NAME): Promise<EventEnvelope> {
    const queue = this.queue.get(queueId);
    if (queue && queue.length > 0) {
      return queue.shift() as EventEnvelope;
    }
    throw new Error(`No messages available in queue ${queueId}.`);
  }

  /**
   * Remove an envelope by identity.
   *
   * Matches, in order:
   *   1. `header.deliveryId` — the handle returned by `enqueue` (the normal path);
   *   2. `header.id` — the business id, which `QueueListener` and the `#deleteMessage`
   *      REST surfaces pass, and which they cannot know a provider handle for;
   *   3. a legacy `<queue>-<index>` handle, for envelopes enqueued by an older
   *      build in the same process.
   *
   * Deletion is by value, never positionally, so an earlier removal cannot make a
   * later handle point at the wrong envelope. `options.queueId` narrows the search
   * when the caller knows it; otherwise every queue is scanned, because a stale
   * handle from a previous cycle may name a queue the envelope has since left.
   */
  async deleteMessage(id: string, options?: DeleteMessageOptions): Promise<void> {
    if (typeof id !== 'string' || id.length === 0) return;

    const prefersQueue = options?.queueId ? this.queue.get(options.queueId) : undefined;
    const candidates: EventEnvelope[][] = prefersQueue ? [prefersQueue] : [...this.queue.values()];

    const removeFirstMatch = (predicate: (envelope: any) => boolean): boolean => {
      for (const queue of candidates) {
        const index = queue.findIndex(predicate);
        if (index >= 0) {
          queue.splice(index, 1);
          return true;
        }
      }
      return false;
    };

    if (removeFirstMatch((envelope) => envelope?.header?.deliveryId === id)) return;
    if (removeFirstMatch((envelope) => envelope?.header?.id === id)) return;

    // Legacy positional handle: `<queueName>-<index>`.
    const legacy = /^(.*)-(\d+)$/.exec(id);
    if (legacy) {
      const queue = this.queue.get(legacy[1]);
      const index = parseInt(legacy[2], 10);
      if (queue && Number.isInteger(index) && index >= 0 && index < queue.length) {
        queue.splice(index, 1);
      }
    }
  }

  async deleteMessages(ids: string[], options?: DeleteMessageOptions): Promise<void> {
    await Promise.all(ids.map(id => this.deleteMessage(id, options)));
  }

  async receiveMessages(options?: ReceiveMessageOptions): Promise<EventEnvelope[]> {
    const queueName = options?.queueId || DEFAULT_QUEUE_NAME;
    const queue = this.queue.get(queueName);
    if (!queue) throw new Error(`Queue ${queueName} does not exist.`);
    const maxMessages = options?.max ?? queue.length;
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

  async onStartup(): Promise<void> {
    const { context } = this;

    // Enqueue a health check message
    const healthCheckMessage: EventEnvelope = {
      header: {
        id: `health-check-${Date.now()}`,
        receivedTimestamp: Date.now(),
        provider: this.provider
      },
      body: {
        validationResults: ['Health check message']
      }
    };
    
    await this.enqueue(healthCheckMessage, { queueId: HEALTH_CHECK_QUEUE_NAME });

    context.log('Health check message enqueued to in-memory queue', 'InMemoryQueueService.onStartup');

    // Process the health check message
    const healthCheckQueue = this.queue.get(HEALTH_CHECK_QUEUE_NAME);
    if (healthCheckQueue && healthCheckQueue.length > 0) {
      const message = healthCheckQueue.shift();
      context.log('Processing health check message', message, 'InMemoryQueueService.onStartup');
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
