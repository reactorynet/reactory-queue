/**
 * Envelope identity in the in-memory provider.
 *
 * REGRESSION — the delivery handle used to be `<queueName>-<index>`, and an array
 * index is not an identity: removing any envelope shifts every later one. Two
 * failures followed, and the second is why the logs filled with
 * `Envelope skipped: message is unknown or already terminal` on a loop:
 *
 *   1. a handle minted before a removal pointed at a DIFFERENT envelope, so
 *      `deleteMessage` removed work that still had to be sent;
 *   2. once a handle ran past the end, `queue[index]` was undefined and
 *      `deleteMessage` silently did nothing — so the envelope could never be
 *      settled and was re-peeked on every poll, forever.
 *
 * `receiveMessages` peeks (does not remove), so settlement is the only way an
 * envelope ever leaves the queue. That is what makes (2) permanent rather than
 * self-correcting.
 */
import { InMemoryQueueService } from '../InMemoryQueueService';

const QUEUE = 'communicator.retry';
const OTHER_QUEUE = 'communicator.primary';

const envelope = (id: string, payload?: any) =>
  ({
    header: { id, receivedTimestamp: Date.now(), provider: 'communicator' },
    body: payload ?? { messageId: id },
  }) as any;

describe('InMemoryQueueService envelope identity', () => {
  let service: InMemoryQueueService;

  beforeEach(() => {
    service = new InMemoryQueueService({} as any, {} as any);
  });

  const contents = (queueId = QUEUE) =>
    (service.queue.get(queueId) || []).map((entry: any) => entry.header.id);

  describe('handles', () => {
    it('are unique, even within one queue', async () => {
      const a = await service.enqueue(envelope('a'), { queueId: QUEUE, createIfNotExists: true });
      const b = await service.enqueue(envelope('b'), { queueId: QUEUE, createIfNotExists: true });

      expect(a).not.toBe(b);
    });

    it('carry a namespace that cannot collide with a business id', async () => {
      const handle = await service.enqueue(envelope('msg-1'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });

      // '#' never appears in a UUID or a business id, so a handle can never be
      // mistaken for `header.id` and vice versa.
      expect(handle).toContain('#');
    });
  });

  describe('deleteMessage', () => {
    it('removes the envelope it was given, not a position', async () => {
      const first = await service.enqueue(envelope('first'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });
      const second = await service.enqueue(envelope('second'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });
      const third = await service.enqueue(envelope('third'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });

      expect(contents()).toEqual(['first', 'second', 'third']);

      // Remove the FIRST. The old index-based handles for `second`/`third` would
      // now be off by one; deleting by identity stays correct.
      await service.deleteMessage(first, { queueId: QUEUE, reason: 'test' });

      expect(contents()).toEqual(['second', 'third']);

      // The handles minted before that removal still name the right envelopes.
      await service.deleteMessage(second, { queueId: QUEUE, reason: 'test' });
      expect(contents()).toEqual(['third']);

      await service.deleteMessage(third, { queueId: QUEUE, reason: 'test' });
      expect(contents()).toEqual([]);
    });

    it('settles every envelope in a batch — the loop that never drained', async () => {
      const handles: string[] = [];
      for (const id of ['t1', 't2', 't3', 't4']) {
        handles.push(
          await service.enqueue(envelope(id), { queueId: QUEUE, createIfNotExists: true }),
        );
      }

      // A skipped envelope is settled by handle. Previously the highest handle
      // fell out of range and that envelope survived every sweep.
      for (const handle of handles) {
        await service.deleteMessage(handle, { queueId: QUEUE, reason: 'skipped' });
      }

      expect(contents()).toEqual([]);
    });

    it('accepts a business id, which is all QueueListener can pass', async () => {
      await service.enqueue(envelope('business-id-1'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });
      await service.enqueue(envelope('business-id-2'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });

      // QueueListener deletes by `message.header.id`; it never sees the handle.
      await service.deleteMessage('business-id-1', { queueId: QUEUE, reason: 'listener' });

      expect(contents()).toEqual(['business-id-2']);
    });

    it('finds an envelope whose queue the caller did not name', async () => {
      const handle = await service.enqueue(envelope('moved'), {
        queueId: OTHER_QUEUE,
        createIfNotExists: true,
      });

      // No queueId in options: scan every queue rather than guess.
      await service.deleteMessage(handle, { queueId: undefined as any, reason: 'no-hint' });

      expect(contents(OTHER_QUEUE)).toEqual([]);
    });

    it('does nothing for an unknown or empty id instead of deleting something else', async () => {
      await service.enqueue(envelope('keep-me'), { queueId: QUEUE, createIfNotExists: true });

      await service.deleteMessage('not-a-real-handle', { queueId: QUEUE, reason: 'test' });
      await service.deleteMessage('', { queueId: QUEUE, reason: 'test' });
      await service.deleteMessage(undefined as any, { queueId: QUEUE, reason: 'test' });

      expect(contents()).toEqual(['keep-me']);
    });

    it('still honours a legacy positional handle', async () => {
      await service.enqueue(envelope('one'), { queueId: QUEUE, createIfNotExists: true });
      await service.enqueue(envelope('two'), { queueId: QUEUE, createIfNotExists: true });

      // An envelope enqueued by an older build in this process carried
      // `<queue>-<index>`; that form must keep working.
      await service.deleteMessage(`${QUEUE}-1`, { queueId: QUEUE, reason: 'legacy' });

      expect(contents()).toEqual(['one']);
    });
  });

  describe('receiveMessages', () => {
    it('peeks without removing, so settlement is the only exit', async () => {
      await service.enqueue(envelope('p1'), { queueId: QUEUE, createIfNotExists: true });

      const first = await service.receiveMessages({ queueId: QUEUE, max: 10 });
      const second = await service.receiveMessages({ queueId: QUEUE, max: 10 });

      expect(first.map((e: any) => e.header.id)).toEqual(['p1']);
      expect(second.map((e: any) => e.header.id)).toEqual(['p1']);
    });

    it('stamps the delivery handle on the envelope a consumer receives', async () => {
      const handle = await service.enqueue(envelope('p2'), {
        queueId: QUEUE,
        createIfNotExists: true,
      });

      const [received] = await service.receiveMessages({ queueId: QUEUE, max: 10 });

      expect((received as any).header.deliveryId).toBe(handle);
    });
  });
});
