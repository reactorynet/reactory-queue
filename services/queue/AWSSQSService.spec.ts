import { AWSSQSQueueService } from './AWSSQSService';

/**
 * The service is registered unconditionally by the reactory-queue module, so its
 * `onStartup` runs on every boot even when SQS is not in use. These tests pin the
 * guard that keeps an unconfigured deployment from attempting a health check
 * against the module's placeholder queue (which produced a
 * `CredentialsProviderError: Could not load credentials from any providers` on
 * every start).
 */
const createContext = () => ({
  log: jest.fn<void, any[]>(),
  warn: jest.fn<void, any[]>(),
  error: jest.fn<void, any[]>(),
});

/**
 * `SQSClient.send` is an overloaded generic, so `jest.spyOn` collapses the spy's
 * parameter type to `never`. Pin it to a plain `jest.Mock` to mock its result.
 */
const spyOnSend = (service: AWSSQSQueueService): jest.Mock =>
  jest.spyOn(service.sqsClient, 'send') as unknown as jest.Mock;

describe('AWSSQSQueueService.onStartup', () => {
  const KEYS = ['DEFAULT_SQS_QUEUE_URL', 'HEALTH_CHECK_SQS_QUEUE_URL'];
  let saved: Record<string, string | undefined>;

  beforeEach(() => {
    saved = {};
    for (const k of KEYS) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  });

  afterEach(() => {
    for (const k of KEYS) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
    jest.restoreAllMocks();
  });

  it('skips the health check and warns when SQS is not configured', async () => {
    const context = createContext();
    const service = new AWSSQSQueueService({}, context as any);
    const send = spyOnSend(service);
    send.mockResolvedValue({});

    await service.onStartup();

    expect(send).not.toHaveBeenCalled();
    expect(context.warn).toHaveBeenCalledWith(
      expect.stringContaining('not configured'),
      'AWSSQSQueueService.onStartup',
    );
    expect(context.error).not.toHaveBeenCalled();
  });

  it('does not treat the placeholder URLs as configuration', async () => {
    // The module falls back to ...default-region.amazonaws.com/123456789012/... —
    // constructing the service must not by itself make it "configured".
    const context = createContext();
    const service = new AWSSQSQueueService({}, context as any);

    expect(service.queueUrl).toContain('default-region');
    expect((service as any).isConfigured()).toBe(false);
  });

  it('sends the health check when a queue URL is configured', async () => {
    const context = createContext();
    const service = new AWSSQSQueueService(
      { queueUrl: 'https://sqs.eu-west-1.amazonaws.com/123456789012/real' },
      context as any,
    );
    const send = spyOnSend(service);
    send.mockResolvedValue({ MessageId: 'ok' });

    await service.onStartup();

    expect(send).toHaveBeenCalledTimes(1);
    expect(context.log).toHaveBeenCalledWith(
      expect.stringContaining('Health check message sent'),
      'AWSSQSQueueService.onStartup',
    );
  });

  it('logs (not throws) when a configured SQS send fails', async () => {
    const context = createContext();
    const service = new AWSSQSQueueService(
      { queueUrl: 'https://sqs.eu-west-1.amazonaws.com/123456789012/real' },
      context as any,
    );
    spyOnSend(service).mockRejectedValue(new Error('network'));

    await expect(service.onStartup()).resolves.toBeUndefined();
    expect(context.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed to send health check message'),
      expect.any(Error),
      'AWSSQSQueueService.onStartup',
    );
  });
});
