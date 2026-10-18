import Reactory from "@reactorynet/reactory-core";
/**
 * AWS SQS Service Implementation
 * 
 * This service requires @aws-sdk/client-sqs to be installed as an optional dependency.
 * To use this service, install it with:
 * yarn add @aws-sdk/client-sqs
 * 
 * @requires @aws-sdk/client-sqs
 */
// @ts-ignore - Optional dependency, may not be installed
import { 
  SQSClient,
  SendMessageCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  CreateQueueCommand
} from '@aws-sdk/client-sqs';
import { service } from "@reactory/server-core/application/decorators";
import { 
  DeleteMessageOptions, 
  EventEnvelope, 
  QueueServiceType, 
  IEnqueueOptions, 
  ReceiveMessageOptions 
} from './types';

const {
  DEFAULT_SQS_QUEUE_URL = 'https://sqs.default-region.amazonaws.com/123456789012/defaultQueue',
  HEALTH_CHECK_SQS_QUEUE_URL = 'https://sqs.default-region.amazonaws.com/123456789012/healthCheckQueue',
  /**
   * SQS is eventually consistent after `CreateQueue`: `SendMessage` can still
   * report QueueDoesNotExist for roughly a second on a freshly created queue.
   * The create path waits this long before re-resolving and retrying once.
   */
  SQS_QUEUE_CREATION_PROPAGATION_MS = '1000'
} = process.env;

/** Result of resolving `IEnqueueOptions.queueId` to an SQS queue URL. */
interface ResolvedQueue {
  queueUrl: string
  /** True when this call created the queue (so a 404 on send may be propagation). */
  created: boolean
}


@service({
  id: 'reactory.AWSSQSQueueService@1.0.0',
  nameSpace: 'reactory',
  name: 'AWSSQSQueueService',
  version: '1.0.0',
  description: "A queue wrapper service that provides a light abstraction for AWS SQS queue services",
  dependencies: [],
  serviceType: 'data',
  roles: ['USER']
})
export class AWSSQSQueueService implements QueueServiceType {
  
  description?: string;
  tags?: string[];
  nameSpace: string;
  name: string;
  version: string;

  props: any;
  context: Reactory.Server.IReactoryContext;

  provider: string = 'aws-sqs';
  sqsClient: SQSClient;
  queueUrl: string;
  healthCheckQueueUrl: string;

  /** Resolved queue id -> SQS queue URL, so lookup/creation happens once. */
  private queueUrls: Map<string, string> = new Map();

  /** Delay before re-resolving after `CreateQueue`; overridable for tests. */
  private createQueuePropagationMs: number;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;

    // Initialize SQS client
    this.sqsClient = new SQSClient({
      region: props?.region || process.env.AWS_REGION || 'us-east-1',
      credentials: props?.credentials || undefined
    });

    // Initialize queue URLs
    this.queueUrl = props?.queueUrl || DEFAULT_SQS_QUEUE_URL;
    this.healthCheckQueueUrl = props?.healthCheckQueueUrl || HEALTH_CHECK_SQS_QUEUE_URL;
    this.createQueuePropagationMs = Number.isFinite(props?.createQueuePropagationMs)
      ? props.createQueuePropagationMs
      : parseInt(SQS_QUEUE_CREATION_PROPAGATION_MS, 10);
  }

  /**
   * Only a genuine "queue does not exist" may be answered with `CreateQueue`.
   * AccessDenied, throttling, expired credentials and network failures must
   * surface unchanged, otherwise a misconfiguration looks like a missing queue
   * and every enqueue attempts a doomed create.
   */
  private isQueueDoesNotExist(error: unknown): boolean {
    const err = error as {
      name?: string
      Code?: string
      code?: string
      message?: string
      $metadata?: { httpStatusCode?: number }
    } | null | undefined;

    const code = err?.name || err?.Code || err?.code || '';
    if (code === 'QueueDoesNotExist' || code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return true;
    }

    if (err?.$metadata?.httpStatusCode === 404) return true;

    return /QueueDoesNotExist|NonExistentQueue/.test(String(err?.message ?? ''));
  }

  private async wait(ms: number): Promise<void> {
    if (!Number.isFinite(ms) || ms <= 0) return;
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Resolve the SQS queue URL addressed by `options.queueId`.
   *
   * A queue id that already is a URL is used verbatim (matching
   * `receiveMessages`, which treats `queueId` as a queue URL). A bare queue id
   * is looked up by name and, when `createIfNotExists` is set, created — that is
   * what makes the option meaningful for SQS, which otherwise addresses a
   * single pre-provisioned queue URL.
   *
   * `forceRefresh` bypasses the per-id cache; it is used after a create when
   * `SendMessage` reported the queue as missing (propagation lag).
   */
  private async resolveQueueUrl(
    options?: IEnqueueOptions,
    forceRefresh = false
  ): Promise<ResolvedQueue> {
    const queueId = options?.queueId;
    if (!queueId) return { queueUrl: this.queueUrl, created: false };

    if (/^https?:\/\//i.test(queueId)) return { queueUrl: queueId, created: false };

    if (!forceRefresh) {
      const cached = this.queueUrls.get(queueId);
      if (cached) return { queueUrl: cached, created: false };
    }

    let queueUrl: string | undefined;
    try {
      const response = await this.sqsClient.send(
        new GetQueueUrlCommand({ QueueName: queueId })
      );
      queueUrl = response?.QueueUrl;
    } catch (error) {
      if (!this.isQueueDoesNotExist(error)) {
        // Real AWS problem (credentials, permissions, throttling, network):
        // log with context and let the original error through.
        this.context?.error?.(
          `Failed to resolve AWS SQS queue '${queueId}'`,
          error as Error,
          'AWSSQSQueueService.resolveQueueUrl'
        );
        throw error;
      }
    }

    if (!queueUrl) {
      if (!options?.createIfNotExists) {
        throw new Error(`Queue ${queueId} does not exist.`);
      }

      const response = await this.sqsClient.send(
        new CreateQueueCommand({ QueueName: queueId })
      );
      queueUrl = response?.QueueUrl;

      if (!queueUrl) {
        throw new Error(`Queue ${queueId} could not be resolved after CreateQueue.`);
      }

      this.queueUrls.set(queueId, queueUrl);
      return { queueUrl, created: true };
    }

    this.queueUrls.set(queueId, queueUrl);
    return { queueUrl, created: false };
  }

  private async sendMessage(queueUrl: string, message: EventEnvelope): Promise<string> {
    const command = new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(message)
    });

    const response = await this.sqsClient.send(command);
    return response.MessageId ?? '';
  }

  async enqueue(message: EventEnvelope, options?: IEnqueueOptions): Promise<string> {
    const resolved = await this.resolveQueueUrl(options);

    try {
      return await this.sendMessage(resolved.queueUrl, message);
    } catch (error) {
      // A queue created by this call may not be visible to SendMessage yet
      // (SQS eventual consistency). Re-resolve once and retry, but only on the
      // create path: a queue that already existed must keep surfacing errors.
      if (!resolved.created || !this.isQueueDoesNotExist(error)) {
        throw error;
      }

      await this.wait(this.createQueuePropagationMs);

      const retried = await this.resolveQueueUrl(
        { ...(options ?? {}), createIfNotExists: true },
        true
      );
      return this.sendMessage(retried.queueUrl, message);
    }
  }

  async dequeue(): Promise<EventEnvelope> {
    const messages = await this.receiveMessages({ max: 1 });
    if (messages.length > 0) {
      return messages[0];
    }
    throw new Error("No messages available in queue.");
  }

  async deleteMessage(id: string, options?: DeleteMessageOptions): Promise<void> {
    const command = new DeleteMessageCommand({
      QueueUrl: this.queueUrl,
      ReceiptHandle: id
    });
    await this.sqsClient.send(command);
  }

  async deleteMessages(ids: string[], options?: DeleteMessageOptions): Promise<void> {
    await Promise.all(ids.map(id => this.deleteMessage(id, options)));
  }

  async receiveMessages(options?: ReceiveMessageOptions): Promise<EventEnvelope[]> {
    const command = new ReceiveMessageCommand({
      QueueUrl: options?.queueId || this.queueUrl,
      MaxNumberOfMessages: options?.max ?? 10,
      WaitTimeSeconds: 10
    });

    const response = await this.sqsClient.send(command);
    return (response.Messages ?? []).map((msg: any) => {
      const envelope = JSON.parse(msg.Body ?? '{}') as EventEnvelope;
      // Store receipt handle in header for deletion
      envelope.header.id = msg.ReceiptHandle || envelope.header.id;
      return envelope;
    });
  }

  async count(queueId?: string): Promise<number> {
    // SQS doesn't directly support a message count, but we can approximate
    // with approximateNumberOfMessages from GetQueueAttributes (if needed).
    return -1; // Placeholder: Implement if exact message count is critical
  }

  async createQueue(queueId: string): Promise<boolean> {
    // This could create an SQS queue, but is a stub here for simplicity.
    return true;
  }

  async deleteQueue(queueId: string): Promise<boolean> {
    // SQS queue deletion requires a dedicated call, could be added here if needed.
    return true;
  }

  /**
   * Whether SQS was actually configured.
   *
   * The module's fallback URLs are placeholders (region `default-region`, account
   * `123456789012`), so their presence does not mean SQS is in use. Only env or
   * props supplying a queue URL counts. Without this guard every boot attempts a
   * health check against the placeholder queue and logs
   * `CredentialsProviderError: Could not load credentials from any providers`.
   */
  private isConfigured(): boolean {
    return Boolean(
      this.props?.queueUrl ||
      this.props?.healthCheckQueueUrl ||
      process.env.DEFAULT_SQS_QUEUE_URL ||
      process.env.HEALTH_CHECK_SQS_QUEUE_URL
    );
  }

  async onStartup(): Promise<void> {
    const { context } = this;

    if (!this.isConfigured()) {
      context.warn(
        'AWS SQS is not configured (no queue URL); skipping the startup health check. ' +
          'Set DEFAULT_SQS_QUEUE_URL / HEALTH_CHECK_SQS_QUEUE_URL (and credentials) to enable SQS.',
        'AWSSQSQueueService.onStartup'
      );
      return;
    }

    try {
      // Send a health check message as EventEnvelope
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
      
      const command = new SendMessageCommand({
        QueueUrl: this.healthCheckQueueUrl,
        MessageBody: JSON.stringify(healthCheckMessage)
      });
      await this.sqsClient.send(command);

      context.log('Health check message sent to AWS SQS health check queue', 'AWSSQSQueueService.onStartup');
    } catch (error) {
      context.error('Failed to send health check message to AWS SQS', error, 'AWSSQSQueueService.onStartup');
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
