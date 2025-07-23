import { APIGatewayProxyEvent, APIGatewayProxyResult, SQSEvent } from "aws-lambda";
import { v4 as uuidv4 } from 'uuid';
import Joi from 'joi';
import createDynamoClient from "./lib/dynamoDB/create-dynamo-client";
import { logger } from "./lib/logger";
import sendResponse from "./lib/serverless/send-response";
import { StandardError, transformError } from "./lib/serverless/error-handling";
import getRequestFromEvent from "./lib/serverless/get-request";
import sqs from "./utils/sqs";
import { JOB_QUEUE_NAME } from "./consts";

const tableName = process.env.PRODUCT_TABLE_NAME || "productTable";

const dynamoClient = createDynamoClient();


export const healthCheck = async (): Promise<unknown> => {
  return sendResponse(200, { message: 'Service is up and running' });
}
const productSchema = Joi.object({
  name: Joi.string().required(),
  description: Joi.string().required(),
  price: Joi.number().required(),
  available: Joi.boolean().required(),
});



export const createProduct = async (event: APIGatewayProxyEvent): Promise<unknown> => {
  try {
    const {
      body,
    } = getRequestFromEvent(event);

    const validatedBody = await productSchema.validateAsync(body);

    const productID = uuidv4();
    const item = {
      PK: `PRODUCT`,
      SK: `PRODUCT#${productID}`,
      ...validatedBody,
    };
    logger.info('Creating item', { item });
    await dynamoClient.put({ TableName: tableName, Item: item });
    return sendResponse(201, item);
  } catch (error) {
    const e = transformError(error)

    logger.error(e.message, { type: e.type, stack: e.stack })

    return await sendResponse(e.statusCode, {
      type: e.type,
      message: e.message,
    })
  }
};

export const getProduct = async (event: APIGatewayProxyEvent): Promise<unknown> => {
  try {
    const {
      params: { id },
    } = getRequestFromEvent(event);

    if (!id) throw new StandardError('Product ID is required', 'CLIENT_ERROR', 400)

    const key = {
      PK: `PRODUCT`,
      SK: `PRODUCT#${id}`,
    };
    logger.info('Retrieving product', { key });
    const { Item } = await dynamoClient.get({ TableName: tableName, Key: key });
    if (!Item) throw new StandardError('Product not found', 'CLIENT_ERROR', 404);

    return sendResponse(200, Item);
  } catch (error) {
    const e = transformError(error)

    logger.error(e.message, { type: e.type, stack: e.stack })

    return await sendResponse(e.statusCode, {
      type: e.type,
      message: e.message,
    })
  }
};

export const updateProduct = async (event: APIGatewayProxyEvent): Promise<unknown> => {
  try {
    const {
      params: { id },
      body
    } = getRequestFromEvent(event);

    const validatedBody = await productSchema.validateAsync(body);

    const key = {
      PK: `PRODUCT`,
      SK: `PRODUCT#${id}`,
    };
    logger.info('Updating product', { key });
    const { Item } = await dynamoClient.get({ TableName: tableName, Key: key });
    if (!Item) throw new StandardError('Product not found', 'CLIENT_ERROR', 404);

    const updatedItem = { ...Item, ...validatedBody };
    await dynamoClient.put({ TableName: tableName, Item: updatedItem });
    return sendResponse(200, updatedItem);
  } catch (error) {
    const e = transformError(error)

    logger.error(e.message, { type: e.type, stack: e.stack })

    return await sendResponse(e.statusCode, {
      type: e.type,
      message: e.message,
    })
  }
};

export const deleteProduct = async (event: APIGatewayProxyEvent): Promise<unknown> => {
  try {
    const {
      params: { id },
    } = getRequestFromEvent(event);

    if (!id) throw new StandardError('Product ID is required', 'CLIENT_ERROR', 400)

    const key = {
      PK: `PRODUCT`,
      SK: `PRODUCT#${id}`,
    };
    logger.info('Deleting product', { key });
    await dynamoClient.delete({ TableName: tableName, Key: key });
    return sendResponse(204, {});
  } catch (error) {
    const e = transformError(error)

    logger.error(e.message, { type: e.type, stack: e.stack })

    return await sendResponse(e.statusCode, {
      type: e.type,
      message: e.message,
    })
  }
};


export const getProducts = async (event: APIGatewayProxyEvent): Promise<unknown> => {
  try {
    const { Items } = await dynamoClient.query({
      TableName: tableName,
      KeyConditionExpression: 'PK = :pk',
      ExpressionAttributeValues: {
        ':pk': 'PRODUCT'
      }
    });

    if (!Items) throw new StandardError('No products found', 'CLIENT_ERROR', 404);

    logger.info('Fetched products', { count: Items.length });
    return sendResponse(200, Items);
  } catch (error) {
    const e = transformError(error)

    logger.error(e.message, { type: e.type, stack: e.stack })

    return await sendResponse(e.statusCode, {
      type: e.type,
      message: e.message,
    })

  }
};

export const enqueueJob = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  const { body } = getRequestFromEvent(event);
  await sqs.publishMessages(JOB_QUEUE_NAME, [body]);
  return sendResponse(202, { status: 'queued' });
};

export const processJob = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    const payload = JSON.parse(record.body);
    logger.info('Processing job', { payload });
  }
};

