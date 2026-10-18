/**
 * Contract tests for `IEnqueueOptions.createIfNotExists`.
 *
 * The option has always been part of the queue contract and is passed by live
 * callers (the communicator and product event publishers), but no provider
 * honoured it: an in-memory enqueue to a new queue id still threw
 * `Queue <id> does not exist.`. These tests pin the behaviour — and the
 * behaviour that must NOT change — for every provider.
 *
 * Provider-specific notes:
 * - In-memory: has no implicit creation, so the flag must create the queue.
 * - BullMQ: materialises a queue on first `add`, so the option is inherently
 *   satisfied; the provider keeps its documented single `queue` slot and is
 *   still targeted with `createQueue(queueId)`, not `enqueue(queueId)`.
 * - SQS: resolves `queueId` by name and creates it, but only for a genuine
 *   "queue does not exist" error, and it tolerates creation propagation lag.
 */

import { InMemoryQueueService } from './InMemoryQueueService';
import { BullMessageQueueService } from './BullMessageQueueService';
import { AWSSQSQueueService } from './AWSSQSService';
import { EventEnvelope } from './types';

// --- BullMQ mock -------------------------------------------------------------
// Constructing a real Queue would open a Redis connection; the unit under test
// only needs to observe which queue received the job and with which options.
jest.mock('bullmq', () => {
  const queues = new Map<string, any>();
  const workers: any[] = [];

  const makeQueue = (name: string) => {
    const queue: any = {
      name,
      add: jest.fn().mockResolvedValue({ id: `${name}-job-id` }),
      getJob: jest.fn(),
      getJobs: jest.fn().mockResolvedValue([]),
      count: jest.fn().mockResolvedValue(0),
      drain: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
    };
    queues.set(name, queue);
    return queue;
  };

  const Queue = jest.fn().mockImplementation((name: string) => queues.get(name) ?? makeQueue(name));

  const Worker = jest.fn().mockImplementation(() => {
    const worker: any = { on: jest.fn(), close: jest.fn().mockResolvedValue(undefined) };
    workers.push(worker);
    return worker;
  });

  return {
    __esModule: true,
    Queue,
    Worker,
    __queues: queues,
    __workers: workers,
    __makeQueue: makeQueue,
  };
});

// eslint-disable-next-line @typescript-eslint/no-var-requires
const bullmq = require('bullmq') as {
  Queue: jest.Mock;
  Worker: jest.Mock;
  __queues: Map<string, any>;
  __workers: any[];
};

const createContext = () => ({
  log: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
  info: jest.fn(),
  getService: jest.fn(),
});

const envelope = (id = 'msg-1'): EventEnvelope => ({
  header: { id, receivedTimestamp: Date.now(), provider: 'test' },
  body: { validationResults: [] },
});

/** An error shaped like the AWS SDK's queue-not-found response. */
const missError = (name = 'QueueDoesNotExist') =>
  Object.assign(new Error(name), { name });

describe('IEnqueueOptions.createIfNotExists', () => {
  describe('InMemoryQueueService', () => {
    let service: InMemoryQueueService;
    let context: ReturnType<typeof createContext>;

    beforeEach(() => {
      context = createContext();
      service = new InMemoryQueueService({}, context as any);
    });

    it('creates an unknown queue when the flag is set and enqueues the message', async () => {
      const message = envelope('created');

      const id = await service.enqueue(message, {
        queueId: 'brand-new-queue',
        createIfNotExists: true,
      });

      expect(id).toBe('brand-new-queue-0');
      expect(await service.count('brand-new-queue')).toBe(1);
      // Proves the message landed on the requested queue, not merely somewhere.
      await expect(service.dequeue('brand-new-queue')).resolves.toEqual(message);
    });

    it('still throws for an unknown queue when the flag is omitted', async () => {
      await expect(
        service.enqueue(envelope(), { queueId: 'unknown-queue' })
      ).rejects.toThrow('Queue unknown-queue does not exist.');
    });

    it('still throws for an unknown queue when the flag is false', async () => {
      await expect(
        service.enqueue(envelope(), { queueId: 'unknown-queue', createIfNotExists: false })
      ).rejects.toThrow('Queue unknown-queue does not exist.');
    });

    it('reuses an existing queue rather than resetting it', async () => {
      await service.createQueue('existing-queue');
      await service.enqueue(envelope('first'), { queueId: 'existing-queue', createIfNotExists: true });
      await service.enqueue(envelope('second'), { queueId: 'existing-queue', createIfNotExists: true });

      expect(await service.count('existing-queue')).toBe(2);
    });

    it('falls back to the default queue when no options are supplied', async () => {
      const id = await service.enqueue(envelope('default'));

      expect(id).toBe('defaultQueue-0');
      expect(context.error).not.toHaveBeenCalled();
    });

    it('drains messages in order and reports an empty queue', async () => {
      const first = envelope('one');
      const second = envelope('two');
      await service.enqueue(first);
      await service.enqueue(second);

      await expect(service.dequeue()).resolves.toEqual(first);
      await expect(service.dequeue()).resolves.toEqual(second);
      await expect(service.dequeue()).rejects.toThrow('No messages available in queue defaultQueue.');
    });

    it('deletes a single message and a batch by id', async () => {
      await service.createQueue('deletes');
      await service.enqueue(envelope('a'), { queueId: 'deletes' });
      await service.enqueue(envelope('b'), { queueId: 'deletes' });
      await service.enqueue(envelope('c'), { queueId: 'deletes' });

      await service.deleteMessage('deletes-1');
      expect(await service.count('deletes')).toBe(2);

      await service.deleteMessages(['deletes-0', 'deletes-1']);
      // 'deletes-1' no longer existed; deleting it is a no-op, not an error.
      expect(await service.count('deletes')).toBe(1);
    });

    it('peeks at messages up to the requested maximum', async () => {
      await service.createQueue('peek');
      await service.enqueue(envelope('a'), { queueId: 'peek' });
      await service.enqueue(envelope('b'), { queueId: 'peek' });
      await service.enqueue(envelope('c'), { queueId: 'peek' });

      const limited = await service.receiveMessages({ queueId: 'peek', max: 2 });
      expect(limited.map(m => m.header.id)).toEqual(['a', 'b']);
      // Peeking must not remove anything.
      expect(await service.count('peek')).toBe(3);

      const all = await service.receiveMessages({ queueId: 'peek' });
      expect(all).toHaveLength(3);
    });

    it('throws when peeking an unknown queue and counts unknown queues as zero', async () => {
      await expect(service.receiveMessages({ queueId: 'nope' })).rejects.toThrow(
        'Queue nope does not exist.'
      );
      await expect(service.count('nope')).resolves.toBe(0);
    });

    it('reports whether createQueue and deleteQueue changed anything', async () => {
      await expect(service.createQueue('lifecycle')).resolves.toBe(true);
      await expect(service.createQueue('lifecycle')).resolves.toBe(false);
      await expect(service.deleteQueue('lifecycle')).resolves.toBe(true);
      await expect(service.deleteQueue('lifecycle')).resolves.toBe(false);
    });

    it('drains the health check queue on startup', async () => {
      await service.onStartup();

      expect(context.log).toHaveBeenCalledWith(
        'Health check message enqueued to in-memory queue',
        'InMemoryQueueService.onStartup',
      );
      expect(await service.count('healthCheckQueue')).toBe(0);
    });

    it('exposes its identity and execution context', () => {
      service.nameSpace = 'reactory';
      service.name = 'InMemoryQueueService';
      service.version = '1.0.0';
      const next = createContext();

      expect(service.toString()).toBe('reactory.InMemoryQueueService');
      expect(service.toString(true)).toBe('reactory.InMemoryQueueService@1.0.0');
      expect(service.getExecutionContext()).toBe(context);
      service.setExecutionContext(next as any);
      expect(service.getExecutionContext()).toBe(next);
    });
  });

  describe('BullMessageQueueService', () => {
    let service: BullMessageQueueService;

    const queueNamed = (name: string) => {
      const queue = bullmq.__queues.get(name);
      if (!queue) throw new Error(`mock queue ${name} was not created`);
      return queue;
    };

    beforeEach(() => {
      bullmq.__queues.clear();
      bullmq.__workers.length = 0;
      bullmq.Queue.mockClear();
      bullmq.Worker.mockClear();
      service = new BullMessageQueueService({}, createContext() as any);
    });

    it('accepts createIfNotExists without a separate create, because BullMQ creates on first add', async () => {
      const message = envelope();

      const id = await service.enqueue(message, {
        queueId: 'communicator.primary',
        createIfNotExists: true,
        priority: 3,
      });

      expect(id).toBe('defaultQueue-job-id');
      expect(queueNamed('defaultQueue').add).toHaveBeenCalledWith('enqueueJob', message, {
        priority: 3,
      });
    });

    it('keeps the documented single queue slot: queueId does not re-target enqueue', async () => {
      // The read paths (receiveMessages/count/deleteMessage) use `this.queue`,
      // so enqueue must too — routing it by queueId would strand messages.
      await service.enqueue(envelope(), { queueId: 'communicator.retry', createIfNotExists: true });

      expect(queueNamed('defaultQueue').add).toHaveBeenCalledTimes(1);
      expect(bullmq.__queues.has('communicator.retry')).toBe(false);
    });

    it('strips queue-contract fields but forwards genuine BullMQ job options', async () => {
      await service.enqueue(envelope(), {
        queueId: 'whatever',
        createIfNotExists: true,
        responseQueueId: 'response',
        failureQueueId: 'failed',
        delay: 250,
        attempts: 2,
      });

      expect(queueNamed('defaultQueue').add).toHaveBeenCalledWith(
        'enqueueJob',
        expect.anything(),
        { delay: 250, attempts: 2 },
      );
    });

    it('passes no options through when none are supplied', async () => {
      await service.enqueue(envelope());

      expect(queueNamed('defaultQueue').add).toHaveBeenCalledWith('enqueueJob', expect.anything(), {});
    });

    it('targets a different queue after createQueue, which is how callers select one', async () => {
      await service.createQueue('communicator.primary');
      await service.enqueue(envelope('after-select'), {
        queueId: 'communicator.primary',
        createIfNotExists: true,
      });

      expect(queueNamed('communicator.primary').add).toHaveBeenCalledTimes(1);
      expect(queueNamed('defaultQueue').add).not.toHaveBeenCalled();
    });

    it('dequeues and removes a job, and throws when the queue is empty', async () => {
      const job = { data: envelope('job-1'), remove: jest.fn().mockResolvedValue(undefined) };
      queueNamed('defaultQueue').getJob.mockResolvedValue(job);

      await expect(service.dequeue()).resolves.toEqual(job.data);
      expect(job.remove).toHaveBeenCalled();

      queueNamed('defaultQueue').getJob.mockResolvedValue(undefined);
      await expect(service.dequeue()).rejects.toThrow('No message found in queue.');
    });

    it('deletes single messages and batches', async () => {
      const job = { remove: jest.fn().mockResolvedValue(undefined) };
      queueNamed('defaultQueue').getJob.mockImplementation(async (id: string) =>
        id === 'known' ? job : undefined
      );

      await service.deleteMessage('known');
      await service.deleteMessage('missing');
      expect(job.remove).toHaveBeenCalledTimes(1);

      await service.deleteMessages(['known', 'known']);
      expect(job.remove).toHaveBeenCalledTimes(3);
    });

    it('receives job data with the default and explicit batch sizes', async () => {
      const jobs = [{ data: envelope('a') }, { data: envelope('b') }];
      queueNamed('defaultQueue').getJobs.mockResolvedValue(jobs);

      await expect(service.receiveMessages()).resolves.toEqual([envelopeLike('a'), envelopeLike('b')]);
      expect(queueNamed('defaultQueue').getJobs).toHaveBeenLastCalledWith(['active', 'waiting'], 0, 10);

      await service.receiveMessages({ max: 2 });
      expect(queueNamed('defaultQueue').getJobs).toHaveBeenLastCalledWith(['active', 'waiting'], 0, 2);
    });

    it('counts, drains and closes the active queue', async () => {
      queueNamed('defaultQueue').count.mockResolvedValue(7);

      await expect(service.count('ignored-queue-id')).resolves.toBe(7);
      await expect(service.deleteQueue('defaultQueue')).resolves.toBe(true);
      expect(queueNamed('defaultQueue').drain).toHaveBeenCalled();
      expect(queueNamed('defaultQueue').close).toHaveBeenCalled();
    });

    it('enqueues a startup health check and wires the worker callbacks', async () => {
      const context = createContext();
      const fresh = new BullMessageQueueService({}, context as any);

      await fresh.onStartup();

      expect(context.log).toHaveBeenCalledWith('BullMessageQueueService starting');
      expect(bullmq.__queues.get('healthCheckQueue').add).toHaveBeenCalledWith('healthCheck', {
        message: 'Health check message',
      });

      const worker = bullmq.__workers[0];
      const handlers = Object.fromEntries(worker.on.mock.calls);
      handlers.completed();
      expect(worker.close).toHaveBeenCalled();
      expect(context.debug).toHaveBeenCalledWith(
        'Health check message processed successfully',
        null,
        'BullMessageQueueService.onStartup',
      );

      const failure = new Error('boom');
      handlers.failed({ id: 'job-1' }, failure);
      expect(context.error).toHaveBeenCalledWith(
        'Health check job job-1 failed:',
        failure,
        'BullMessageQueueService.onStartup',
      );
    });

    it('exposes its identity and execution context', () => {
      service.nameSpace = 'reactory';
      service.name = 'BullMessageQueueService';
      service.version = '1.0.0';
      const next = createContext();

      expect(service.toString()).toBe('reactory.BullMessageQueueService');
      expect(service.toString(true)).toBe('reactory.BullMessageQueueService@1.0.0');
      expect(service.getExecutionContext()).toBeDefined();
      service.setExecutionContext(next as any);
      expect(service.getExecutionContext()).toBe(next);
    });
  });

  describe('AWSSQSQueueService', () => {
    const defaultUrl = 'https://sqs.eu-west-1.amazonaws.com/123456789012/default';
    const createdUrl = 'https://sqs.eu-west-1.amazonaws.com/123456789012/created';
    const existingUrl = 'https://sqs.eu-west-1.amazonaws.com/123456789012/existing';
    let service: AWSSQSQueueService;
    let context: ReturnType<typeof createContext>;
    let send: jest.Mock;

    const commandNames = () => send.mock.calls.map(([command]) => command.constructor.name);
    const commandInput = (name: string) =>
      (send.mock.calls.find(([command]) => command.constructor.name === name)?.[0] as any)?.input;

    const build = (props: Record<string, unknown> = {}) => {
      context = createContext();
      service = new AWSSQSQueueService({ queueUrl: defaultUrl, ...props }, context as any);
      send = jest.spyOn(service.sqsClient, 'send') as unknown as jest.Mock;
      return service;
    };

    afterEach(() => {
      jest.restoreAllMocks();
    });

    it('creates a missing queue when the flag is set and sends to the created URL', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            throw missError();
          case 'CreateQueueCommand':
            return { QueueUrl: createdUrl };
          case 'SendMessageCommand':
            return { MessageId: 'sqs-message-id' };
          default:
            return {};
        }
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'new-sqs-queue', createIfNotExists: true })
      ).resolves.toBe('sqs-message-id');

      expect(commandNames()).toEqual([
        'GetQueueUrlCommand',
        'CreateQueueCommand',
        'SendMessageCommand',
      ]);
      expect(commandInput('CreateQueueCommand')).toEqual({ QueueName: 'new-sqs-queue' });
      expect(commandInput('SendMessageCommand')).toEqual({
        QueueUrl: createdUrl,
        MessageBody: expect.any(String),
      });
    });

    it('uses an existing queue without creating it', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') return { QueueUrl: existingUrl };
        if (command.constructor.name === 'SendMessageCommand') return { MessageId: 'existing-id' };
        return {};
      });

      await service.enqueue(envelope(), { queueId: 'existing-sqs-queue', createIfNotExists: true });

      expect(commandNames()).toEqual(['GetQueueUrlCommand', 'SendMessageCommand']);
      expect(commandInput('SendMessageCommand')).toEqual({
        QueueUrl: existingUrl,
        MessageBody: expect.any(String),
      });
    });

    it('throws for an unknown queue when the flag is omitted', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') throw missError();
        return {};
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'unknown-sqs-queue' })
      ).rejects.toThrow('Queue unknown-sqs-queue does not exist.');
      expect(commandNames()).toEqual(['GetQueueUrlCommand']);
    });

    it('treats a lookup that returns no URL as missing and throws without the flag', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') return {};
        return {};
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'url-less-queue' })
      ).rejects.toThrow('Queue url-less-queue does not exist.');
      expect(commandNames()).toEqual(['GetQueueUrlCommand']);
    });

    it('creates the queue when the lookup response carries no URL but the flag is set', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            return {};
          case 'CreateQueueCommand':
            return { QueueUrl: createdUrl };
          default:
            return { MessageId: 'created-from-empty-lookup' };
        }
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'empty-lookup-queue', createIfNotExists: true })
      ).resolves.toBe('created-from-empty-lookup');
      expect(commandNames()).toEqual([
        'GetQueueUrlCommand',
        'CreateQueueCommand',
        'SendMessageCommand',
      ]);
    });

    it('reports an unresolvable queue when CreateQueue returns no URL', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') throw missError();
        if (command.constructor.name === 'CreateQueueCommand') return {};
        return {};
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'bad-create', createIfNotExists: true })
      ).rejects.toThrow('Queue bad-create could not be resolved after CreateQueue.');
      expect(commandNames()).toEqual(['GetQueueUrlCommand', 'CreateQueueCommand']);
    });

    it('rethrows non-404 lookup failures instead of masking them as a missing queue', async () => {
      build();
      const accessDenied = Object.assign(new Error('Access to the resource is denied'), {
        name: 'AccessDeniedException',
      });
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') throw accessDenied;
        return {};
      });

      // Even with createIfNotExists, a permissions problem must not become a create.
      await expect(
        service.enqueue(envelope(), { queueId: 'denied-queue', createIfNotExists: true })
      ).rejects.toBe(accessDenied);
      expect(commandNames()).toEqual(['GetQueueUrlCommand']);
      expect(context.error).toHaveBeenCalledWith(
        "Failed to resolve AWS SQS queue 'denied-queue'",
        accessDenied,
        'AWSSQSQueueService.resolveQueueUrl',
      );
    });

    it('classifies the legacy NonExistentQueue code and an HTTP 404 as a missing queue', async () => {
      build();
      let attempt = 0;
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') {
          attempt += 1;
          if (attempt === 1) throw missError('AWS.SimpleQueueService.NonExistentQueue');
          throw Object.assign(new Error('404'), { $metadata: { httpStatusCode: 404 } });
        }
        if (command.constructor.name === 'CreateQueueCommand') return { QueueUrl: createdUrl };
        return { MessageId: 'id' };
      });

      await service.enqueue(envelope(), { queueId: 'legacy-code', createIfNotExists: true });
      await service.enqueue(envelope(), { queueId: 'http-404', createIfNotExists: true });

      expect(send.mock.calls.filter(([c]) => c.constructor.name === 'CreateQueueCommand')).toHaveLength(2);
    });

    it('re-resolves once and retries when a freshly created queue is not visible to SendMessage', async () => {
      build({ createQueuePropagationMs: 0 });
      let resolvedAfterCreate = false;
      send.mockImplementation(async (command: any) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            if (!resolvedAfterCreate) throw missError();
            return { QueueUrl: createdUrl };
          case 'CreateQueueCommand':
            resolvedAfterCreate = true;
            return { QueueUrl: createdUrl };
          case 'SendMessageCommand':
            if (!resolvedAfterCreate) throw missError();
            return { MessageId: 'retried-id' };
          default:
            return {};
        }
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'propagating-queue', createIfNotExists: true })
      ).resolves.toBe('retried-id');

      expect(commandNames()).toEqual([
        'GetQueueUrlCommand',
        'CreateQueueCommand',
        'SendMessageCommand',
      ]);
    });

    it('waits for the configured propagation delay before retrying', async () => {
      build({ createQueuePropagationMs: 1 });
      let sends = 0;

      // Force the create path by making the first lookup miss, then succeed.
      let lookups = 0;
      send.mockImplementation(async (command: any) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            lookups += 1;
            if (lookups === 1) throw missError();
            return { QueueUrl: createdUrl };
          case 'CreateQueueCommand':
            return { QueueUrl: createdUrl };
          case 'SendMessageCommand':
            sends += 1;
            if (sends === 1) throw missError();
            return { MessageId: 'after-delay' };
          default:
            return {};
        }
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'slow-queue', createIfNotExists: true })
      ).resolves.toBe('after-delay');
    });

    it('does not retry a missing-queue error for a queue that already existed', async () => {
      build({ createQueuePropagationMs: 0 });
      const sendError = missError();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') return { QueueUrl: existingUrl };
        if (command.constructor.name === 'SendMessageCommand') throw sendError;
        return {};
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'existing-sqs-queue', createIfNotExists: true })
      ).rejects.toBe(sendError);
      expect(commandNames()).toEqual(['GetQueueUrlCommand', 'SendMessageCommand']);
    });

    it('rethrows the retry failure when the queue is still not visible', async () => {
      build({ createQueuePropagationMs: 0 });
      const retryError = missError();
      send.mockImplementation(async (command: any) => {
        switch (command.constructor.name) {
          case 'GetQueueUrlCommand':
            throw missError();
          case 'CreateQueueCommand':
            return { QueueUrl: createdUrl };
          case 'SendMessageCommand':
            throw retryError;
          default:
            return {};
        }
      });

      await expect(
        service.enqueue(envelope(), { queueId: 'still-missing', createIfNotExists: true })
      ).rejects.toBe(retryError);
    });

    it('caches a resolved queue so repeat enqueues do not re-look it up', async () => {
      build();
      send.mockImplementation(async (command: any) => {
        if (command.constructor.name === 'GetQueueUrlCommand') return { QueueUrl: existingUrl };
        return { MessageId: 'cached' };
      });

      await service.enqueue(envelope('one'), { queueId: 'cached-queue', createIfNotExists: true });
      await service.enqueue(envelope('two'), { queueId: 'cached-queue', createIfNotExists: true });

      expect(send.mock.calls.filter(([c]) => c.constructor.name === 'GetQueueUrlCommand')).toHaveLength(1);
      expect(send.mock.calls.filter(([c]) => c.constructor.name === 'SendMessageCommand')).toHaveLength(2);
    });

    it('uses a queueId that is already a URL verbatim', async () => {
      build();
      send.mockResolvedValue({ MessageId: 'url-passthrough' });

      await expect(
        service.enqueue(envelope(), { queueId: existingUrl, createIfNotExists: true })
      ).resolves.toBe('url-passthrough');

      expect(commandNames()).toEqual(['SendMessageCommand']);
      expect(commandInput('SendMessageCommand')).toEqual({
        QueueUrl: existingUrl,
        MessageBody: expect.any(String),
      });
    });

    it('sends to the configured default queue when no queueId is supplied', async () => {
      build();
      send.mockResolvedValue({ MessageId: 'default-id' });

      await expect(service.enqueue(envelope())).resolves.toBe('default-id');
      expect(commandNames()).toEqual(['SendMessageCommand']);
      expect(commandInput('SendMessageCommand')).toEqual({
        QueueUrl: defaultUrl,
        MessageBody: expect.any(String),
      });
    });

    it('falls back to an empty id when SendMessage returns none', async () => {
      build();
      send.mockResolvedValue({});

      await expect(service.enqueue(envelope())).resolves.toBe('');
    });

    it('dequeues, deletes and receives messages', async () => {
      build();
      send.mockResolvedValue({
        Messages: [
          // Receipt handle replaces the envelope id, which is what deleteMessage expects.
          { Body: JSON.stringify(envelope('received')), ReceiptHandle: 'receipt-1' },
          // Without one, the envelope keeps its own id.
          { Body: JSON.stringify(envelope('no-receipt')) },
        ],
      });

      const messages = await service.receiveMessages({ queueId: existingUrl, max: 2 });
      expect(messages[0].header.id).toBe('receipt-1');
      expect(messages[1].header.id).toBe('no-receipt');
      expect(commandInput('ReceiveMessageCommand')).toEqual({
        QueueUrl: existingUrl,
        MaxNumberOfMessages: 2,
        WaitTimeSeconds: 10,
      });

      await expect(service.dequeue()).resolves.toEqual(messages[0]);

      send.mockResolvedValue({});
      await expect(service.dequeue()).rejects.toThrow('No messages available in queue.');

      send.mockResolvedValue({});
      await service.deleteMessage('receipt-9', { queueId: existingUrl, reason: 'processed' });
      await service.deleteMessages(['a', 'b'], { queueId: existingUrl, reason: 'processed' });
      expect(send.mock.calls.filter(([c]) => c.constructor.name === 'DeleteMessageCommand')).toHaveLength(3);
    });

    it('reports the placeholder answers for unsupported operations', async () => {
      build();

      await expect(service.count()).resolves.toBe(-1);
      await expect(service.createQueue('anything')).resolves.toBe(true);
      await expect(service.deleteQueue('anything')).resolves.toBe(true);
    });

    it('exposes its identity and execution context', () => {
      build();
      service.nameSpace = 'reactory';
      service.name = 'AWSSQSQueueService';
      service.version = '1.0.0';
      const next = createContext();

      expect(service.toString()).toBe('reactory.AWSSQSQueueService');
      expect(service.toString(true)).toBe('reactory.AWSSQSQueueService@1.0.0');
      expect(service.getExecutionContext()).toBe(context);
      service.setExecutionContext(next as any);
      expect(service.getExecutionContext()).toBe(next);
    });
  });
});

/** Build an envelope carrying only the header id, for order assertions. */
function envelopeLike(id: string) {
  return {
    header: { id, receivedTimestamp: expect.any(Number), provider: 'test' },
    body: { validationResults: [] as string[] },
  };
}
