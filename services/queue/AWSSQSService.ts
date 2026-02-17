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
  GetQueueAttributesCommand
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
  HEALTH_CHECK_SQS_QUEUE_URL = 'https://sqs.default-region.amazonaws.com/123456789012/healthCheckQueue'
} = process.env;


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
  }
  
  async enqueue(message: EventEnvelope, options?: IEnqueueOptions): Promise<string> {
    const command = new SendMessageCommand({
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(message)
    });
    
    const response = await this.sqsClient.send(command);
    return response.MessageId ?? '';
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

  async onStartup(): Promise<void> {
    const { context } = this;

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
