/* eslint class-methods-use-this: ["error", { "exceptMethods": ["handleEvent"] }] */

import { InitNodeCache } from '../cache/cache'
import { v4 as uuid } from 'uuid';
import { eachLimit } from 'async';
import {
  SQSClient,
  GetQueueUrlCommand,
  GetQueueUrlCommandOutput,
  SendMessageBatchCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from '@aws-sdk/client-sqs';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

import {
  AWS_DEFAULT_REGION,
  AWS_USE_SQS_LOCAL,
  ENVIRONMENT,
  SQS_LOCAL_ENDPOINT,
  SQS_RECEIVE_WAIT_TIME_SECONDS,
} from '../../consts';
import { logger } from '../logger';
import { SqsGetQueueUrl, SqsHandleEvent, SqsPublish, SqsSubscribe } from '../../types/sqs-types';

export default class SQS {
  cache: InitNodeCache;
  client: SQSClient;
  devClient: LambdaClient | null;

  constructor({ devSubscribeEndpoint }: { devSubscribeEndpoint?: string } = {}) {
    this.cache = new InitNodeCache();

    this.client = new SQSClient({
      region: AWS_DEFAULT_REGION,
      endpoint: AWS_USE_SQS_LOCAL ? SQS_LOCAL_ENDPOINT : undefined,
    });

    this.devClient = ENVIRONMENT === 'development' && devSubscribeEndpoint
      ? new LambdaClient({ region: AWS_DEFAULT_REGION, endpoint: devSubscribeEndpoint })
      : null;
  }

  /**
   * Get the URL of an SQS queue, caching the result for future use.
   * @param queueName - The name of the SQS queue.
   * @returns The URL of the SQS queue.
   */
  getQueueUrl: SqsGetQueueUrl = async (queueName) => {
    logger.debug(`Getting queue URL for ${queueName} from cache`);

    let queueUrl = this.cache.get(queueName) as GetQueueUrlCommandOutput;

    if (!queueUrl) {
      logger.debug(`Queue URL for ${queueName} not found in cache, fetching from SQS`);

      queueUrl = await this.client.send(new GetQueueUrlCommand({ QueueName: queueName }));

      logger.debug(`Fetched queue URL for ${queueName} from SQS, saving to cache`);

      this.cache.set(queueName, queueUrl);

      logger.debug(`Queue URL for ${queueName} saved to cache`);
    }

    logger.debug(`Final queue URL: ${queueUrl.QueueUrl}`);

    return queueUrl.QueueUrl ?? '';
  };

  /**
   * Publish messages to an SQS queue, handling batching and optional local development mode.
   * @param queueName - The name of the SQS queue.
   * @param messages - The messages to publish.
   * @param batchOptions - Optional batch options including batch ID and count.
   */
  publishMessages: SqsPublish = async (queueName, messages, batchOptions) => {
    logger.addExecutionContext({ queue: queueName });

    const batchId = batchOptions?.batchId ?? uuid();
    const batchCount = batchOptions?.batchCount ?? messages.length;

    if (ENVIRONMENT === 'development' && !AWS_USE_SQS_LOCAL) {
      logger.debug('Development environment detected, using Lambda client');

      if (!this.devClient) {
        logger.error('Lambda client is not available, please pass a subscriber endpoint to the SQS class initialization');
        return;
      }

      const command = new InvokeCommand({
        FunctionName: queueName,
        InvocationType: 'RequestResponse',
        Payload: JSON.stringify({
          Records: messages.map((body) => ({
            batchId,
            batchCount,
            messageId: uuid(),
            body: JSON.stringify(body),
            eventSource: '',
            eventSourceARN: '',
            attributes: {
              timestamp: new Date(),
            },
          })),
        }) as unknown as Uint8Array,
      });

      logger.debug(`Publishing message to queue ${queueName}`);

      await this.devClient.send(command);

      logger.debug(`Successfully published message to queue ${queueName}`);
    } else {
      logger.debug('Getting queue URL');

      const queueUrl = await this.getQueueUrl(queueName);

      logger.debug('Successfully got queue URL');

      const command = new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: messages.map((body) => ({
          Id: uuid(),
          MessageBody: JSON.stringify(body),
          MessageAttributes: {
            batchId: {
              DataType: 'String',
              StringValue: batchId,
            },
            batchCount: {
              DataType: 'Number',
              StringValue: batchCount.toString(),
            },
          },
        })),
      });

      logger.debug(`Publishing message to queue ${queueName}`);

      await this.client.send(command);

      logger.debug(`Successfully published message to queue ${queueName}`);
    }
  };

  /**
   * Subscribe to an SQS queue and process messages with a given callback function.
   * @param callback - The function to process messages.
   * @param queue - Optional queue name override.
   */
  subscribeToQueue: SqsSubscribe = async (callback, queue = null) => {
    const queueName = queue ?? `${callback.name.replace(/[A-Z]/g, (capital: string) => `_${capital.toLowerCase()}`)}_${ENVIRONMENT}`;

    logger.addExecutionContext({
      queue: queueName,
    });

    logger.debug(`Getting queue URL for ${queueName}`);

    const queueUrl = await this.getQueueUrl(queueName);

    logger.debug(`Trying to receive message, long polling for ${SQS_RECEIVE_WAIT_TIME_SECONDS} seconds`);

    const receivedData = await this.client.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      WaitTimeSeconds: Number(SQS_RECEIVE_WAIT_TIME_SECONDS && +SQS_RECEIVE_WAIT_TIME_SECONDS <= 20 ? SQS_RECEIVE_WAIT_TIME_SECONDS : 20),
    }));

    const { Messages: messages } = receivedData;

    if (messages && messages.length) {
      const modifiedMessages = messages.map((message) => ({
        attributes: {
          ...message.Attributes,
          ...message.MessageAttributes,
        },
        body: message.Body ? JSON.parse(message.Body) : null,
        messageId: message.MessageId,
        receiptHandle: message.ReceiptHandle,
      }));

      logger.debug(`Successfully got messages from queue ${queueName}, processing with callback`);

      eachLimit(modifiedMessages, 1, async (message) => {
        const { attributes, messageId, receiptHandle, body } = message;

        logger.addExecutionContext({
          attributes,
          messageId,
        });

        try {
          if (!body) {
            logger.debug('Message body is empty, skipping');
            return;
          }

          logger.debug('Processing message with callback');

          await callback(message);

          logger.debug('Successfully processed message, deleting from queue');

          await this.client.send(new DeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: receiptHandle,
          }));

          logger.debug('Successfully deleted message from queue');
          logger.debug('Fetching more messages');

          this.subscribeToQueue(callback, queueName);
        } catch (e) {
          logger.error(`Error processing message with callback: ${JSON.stringify(e)}`);
        }
      });
    } else {
      logger.debug('No new messages, trying again');

      this.subscribeToQueue(callback, queueName);
    }
  };

  /**
   * Handle an SQS event by parsing the message and passing it to a callback function.
   * @param callback - The function to handle the parsed message.
   * @param message - The SQS message to process.
   */
  handleSqsEvent: SqsHandleEvent = async (callback, message) => {
    try {
      const parsed = {
        ...message,
        body: JSON.parse(message.body),
      };

      const { messageId, attributes, eventSource, eventSourceARN } = message;

      logger.addExecutionContext({
        messageId,
        attributes,
        eventSource,
        eventSourceARN,
      });

      logger.debug('Passing message to handler');

      await callback(parsed);

      logger.info('Successfully handled message');
    } catch (e) {
      logger.error('Error handling message', { error: e });
      throw e;
    }
  };
}