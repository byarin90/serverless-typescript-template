/* eslint-disable @typescript-eslint/no-explicit-any */
import Joi from 'joi'
import { APIGatewayProxyResult } from 'aws-lambda'

interface TransformedError {
  statusCode: number;
  type: string;
  message: string;
  stack: string;
}

type TransformError = (e: any) => TransformedError

interface JoiValidationSchemasObject {
  bodySchema?: Joi.Schema
  querySchema?: Joi.Schema
  paramsSchema?: Joi.Schema
}

type JoiValidation = (event: any, schemas: JoiValidationSchemasObject) => Promise<void>
type WithSchema = (schema: Joi.Schema, data: Record<any, any> | Request, dataPath: string) => Promise<void>

interface ParsedRequestObject {
  headers: Record<string, any>
  params: Record<string, any>
  query: Record<string, any>
  body: Record<string, any>
  rawBody: string
  useEncoding?: boolean
}

type GetRequestFromEvent = (event: Record<string, any>) => ParsedRequestObject
type SendResponse = (statusCode?: number, body?: Record<any, any>, headers?: Record<any, any>) => Promise<APIGatewayProxyResult>

export {
  TransformError,
  TransformedError,
  JoiValidation,
  WithSchema,
  ParsedRequestObject,
  GetRequestFromEvent,
  SendResponse,
}
