import { InMemoryQueueService } from "./queue/InMemoryQueueService";
import { BullMessageQueueService } from "./queue/BullMessageQueueService";
import { AWSSQSQueueService } from "./queue/AWSSQSService";
import { QueueProvider } from "./queue/QueueProvider";

export default [
  QueueProvider,
  BullMessageQueueService,
  InMemoryQueueService,
  AWSSQSQueueService,
];
