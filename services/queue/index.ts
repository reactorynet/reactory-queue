/**
 * Reactory Queue Services
 * 
 * This module provides a flexible queue abstraction layer supporting multiple backends.
 * 
 * @module reactory-queue/services/queue
 */

// Core Provider
export { QueueProvider } from './QueueProvider';
export type { IQueueProviderConfig } from './QueueProvider';

// Queue Service Implementations
export { BullMessageQueueService } from './BullMessageQueueService';
export { InMemoryQueueService } from './InMemoryQueueService';
export { AWSSQSQueueService } from './AWSSQSService';

// Types
export type {
  THeader,
  IQueueEnvelope,
  IEnqueueOptions,
  IDequeueOptions,
  CreateQueueOptions,
  DeleteMessageOptions,
  ReceiveMessageOptions,
  IQueueService,
  EnvelopeHeader,
  EventEnvelopeBody,
  EventEnvelope,
  QueueServiceType
} from './types';

