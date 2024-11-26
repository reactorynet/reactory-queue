import Reactory from "@reactory/reactory-core";
import {  
  SQS,

} from 'aws-sdk';
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
} = process.env;s


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
  sqsClient: SQS
  queueUrl: string;
  healthCheckQueueUrl: string;

  constructor(props: any, context: Reactory.Server.IReactoryContext) {
    this.props = props;
    this.context = context;

    // Initialize SQS client and queue URLs
    this.queueUrl = DEFAULT_SQS_QUEUE_URL;
    this.healthCheckQueueUrl = HEALTH_CHECK_SQS_QUEUE_URL;
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
    const messages = await this.receiveMessages({ maxMessages: 1 });
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
      QueueUrl: this.queueUrl,
      MaxNumberOfMessages: options?.max ?? 10,
      WaitTimeSeconds: options?.waitTimeSeconds ?? 10
    });

    const response = await this.sqsClient.send(command);
    return (response.Messages ?? []).map(msg => ({
      ...JSON.parse(msg.Body ?? '{}'),
      receiptHandle: msg.ReceiptHandle
    }));
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

  async onStartup(context: Reactory.Server.IReactoryContext): Promise<void> {
    this.context = context;

    // Send a health check message
    const healthCheckMessage = { message: 'Health check message', timestamp: Date.now() };
    const command = new SendMessageCommand({
      QueueUrl: this.healthCheckQueueUrl,
      MessageBody: JSON.stringify(healthCheckMessage)
    });
    await this.sqsClient.send(command);

    console.log('Health check message sent to health check queue.');
    
    // Optional: Set up periodic health check or message processing as needed
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
