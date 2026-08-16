/** An error carrying the HTTP status it should be reported with. */
export class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly headers?: Record<string, string>,
  ) {
    super(message);
    this.name = 'HttpError';
  }
}

export const badRequest = (message: string): HttpError => new HttpError(400, message);
export const unauthorized = (message: string): HttpError => new HttpError(401, message);
export const forbidden = (message: string): HttpError => new HttpError(403, message);
export const notFound = (message: string): HttpError => new HttpError(404, message);
export const payloadTooLarge = (message: string): HttpError => new HttpError(413, message);

export const tooManyRequests = (message: string, retryAfterSeconds: number): HttpError =>
  new HttpError(429, message, { 'Retry-After': String(retryAfterSeconds) });

export const serviceUnavailable = (message: string, retryAfterSeconds: number): HttpError =>
  new HttpError(503, message, { 'Retry-After': String(retryAfterSeconds) });
