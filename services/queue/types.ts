import { EventPriority, IEvent, EventType } from "@reactory/server-modules/reactory-communicator/models/event/types";

export type THeader = Reactory.IKeyValuePair<string, unknown>;

export interface IQueueEnvelope<THeader, TBody> {
  header: THeader
  body: TBody
}

export interface IEnqueueOptions {
  queueId?: string
  responseQueueId?: string
  failureQueueId?: string
  /**
   * Will create a queue if it does not exist.
   */
  createIfNotExists?: boolean
}

export interface IDequeueOptions {
  queueId: string
}

export interface CreateQueueOptions {
  queueId: string
  description: string
}

export interface DeleteMessageOptions {
  queueId: string
  reason: string
}

export interface ReceiveMessageOptions {
  max?: number
  queueId?: string
}


/**
 * Defines the interface for a queue service
 */
export interface IQueueService<TEnvelope, TId> extends Reactory.Service.IReactoryDefaultService {
  /**
   * Enqueues a message on a queue.
   * @param message 
   * @param options 
   */
  enqueue(message: TEnvelope, options?: any): Promise<TId>;
  /**
   * Removes an envelope from a queue
   */
  dequeue(): Promise<TEnvelope | null>;
  /**
   * Deletes a messages from a queue
   * @param id 
   */
  deleteMessage(id: TId, options?: DeleteMessageOptions): Promise<void>;
  /**
   * Deletes a set of messages from a queue
   * @param ids 
   */
  deleteMessages(ids: TId[], options?: DeleteMessageOptions): Promise<void>;
  /**
   * Receive messages from a queue
   * @param options 
   */
  receiveMessages(options?: ReceiveMessageOptions): Promise<TEnvelope[]>;
  /**
   * Count the number of messages in a queue
   * @param queueId 
   */
  count(queueId?: string):Promise<number>
  /**
   * Creates a queue
   * @param queueId 
   */
  createQueue(queueId: string): Promise<boolean>
  /**
   * Deletes a queue
   * @param queueId
   * @returns <boolean> - false if failed to delete the queue 
   */
  deleteQueue(queueId: string): Promise<boolean>
}

export interface EnvelopeHeader {
  /**
   * Unique Id for the envelope
   */
  id: string
  /**
   * Timestamp
   */
  receivedTimestamp: number
  /**
   * Queue Provider
   */
  provider: string
}

export interface EventEnvelopeBody {
  /**
   * Contains validation results of event.
   */
  validationResults?: string[]
}

export type EventEnvelope = IQueueEnvelope<EnvelopeHeader, EventEnvelopeBody>

export type QueueServiceType = IQueueService<EventEnvelope, string>

// Re-export QueueProvider types
export type { IQueueProviderConfig } from './QueueProvider';