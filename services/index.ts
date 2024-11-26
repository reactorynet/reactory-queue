import { InMemoryQueueService } from "./queue/InMemoryQueueService";
import { BullMessageQueueService } from "./queue/BullMessageQueueService";
export default [
  BullMessageQueueService,
  InMemoryQueueService,
];
