let http = require('http');
let https = require('https');
let assert = require('assert');
let urllib = require('url');
let debug = require('debug')('requests');

let versionString = '/unknownversion';

let pkgInfo = require('../package.json');
versionString = '/' + pkgInfo.version;

let httpAgent = new http.Agent({
  keepAlive: true,
});

let httpsAgent = new https.Agent({
  keepAlive: true,
});

/**
 * Return true if these headers are ones that we'll support and false if they
 * aren't.  We only support headers where there is a single level deep mapping
 * of string to string values
 */
function validateHeaders(headers) {
  if (typeof headers !== 'object') {
    return false;
  }
  for (let key of Object.keys(headers)) {
    if (typeof headers[key] !== 'string') {
      return false;
    }
  }
  return true;
}

class RequestError extends Error {

}

/**
 * Make a request against a URL.  The provided URL will be a correctly formed
 * URL in a string.  This method will not follow any redirects.  This method
 * will return a promise which resolves an object like {response, request}.
 */
async function request(url, opts = {}) {
  assert(typeof url === 'string', 'must provide a url');
  let urlParts = urllib.parse(url);
  if (!opts.allowUnsafeUrls) {
    assert(urlParts.protocol === 'https:', 'only https is supported');
  } else {
    debug('WARNING: allowing unsafe urls');
  }

  let headers = opts.headers || {};
  if (!validateHeaders(headers)) {
    throw new RequestError('Headers are invalid');
  }
  headers['user-agent'] = 'cloud-mirror' + versionString;

  let port;
  try {
    if (urlParts.port) {
      port = parseInt(urlParts.port, 10);
    }
  } catch (err) {
    throw new RequestError('Cannot parse port'); 
  }

  let method = opts.method || 'GET';
  method = method.toUpperCase();

  assert(http.METHODS.indexOf(method) !== -1);
  
  if (opts.stream) {
    assert(typeof opts.contentType === 'string',
        'when supplying a request body, you must specify content-type');
    assert(!opts.data, 'if supplying a stream, you must not supply data');
  }

  if (opts.data) {
    assert(typeof opts.contentType === 'string',
        'when supplying a request body, you must specify content-type');
    assert(!opts.stream, 'if supplying a stream, you must not supply data');
  }

  switch (urlParts.protocol) {
    case 'https:':
      return makeRequest({
        httpLib: https,
        hostname: urlParts.hostname,
        port: port || 443,
        path: urlParts.path || '/',
        headers: headers,
        method: method,
        stream: opts.stream,
        data: opts.data,
        contentType: opts.contentType,
        timeout: opts.timeout || 60000,
      });
      break;
    case 'http:':
      return makeRequest({
        httpLib: http,
        hostname: urlParts.hostname,
        port: port || 80,
        path: urlParts.path || '/',
        headers: headers,
        method: method,
        stream: opts.stream,
        data: opts.data,
        contentType: opts.contentType,
        timeout: opts.timeout || 60000,
      });
      break;
    default:
      throw new RequestError('Invalid protocol: ' + urlParts.protocol);
      break;
  }
  
}

module.exports = {
  request,
  RequestError,
};

async function makeRequest(opts) {
  assert(typeof opts === 'object');
  assert(typeof opts.httpLib === 'object');
  assert(typeof opts.hostname === 'string');
  assert(typeof opts.port === 'number');
  assert(typeof opts.path === 'string');
  assert(typeof opts.method === 'string');
  assert(typeof opts.headers === 'object');
  assert(typeof opts.timeout === 'number');
  if (opts.stream) {
    assert(typeof opts.contentType === 'string');
    assert(typeof opts.stream === 'object');
    assert(typeof opts.stream.pipe === 'function');
  }
  if (opts.data) {
    assert(typeof opts.contentType === 'string');
    assert(typeof opts.data.length === 'number');
  }

  return new Promise(async (resolve, reject) => {
    let request = opts.httpLib.request({
      hostname: opts.hostname,
      port: opts.port,
      path: opts.path,
      method: opts.method,
    });

    // We don't want connections hanging around forever, so we'll set a fatal
    // condition that they timeout if they aren't responded to quickly enough
    request.setTimeout(opts.timeout);

    request.on('error', err => {
      reject(err);
    });

    request.on('socket', socket => {
      debug('have a socket');

      socket.on('error', err => {
        debug('socket error');
        reject(err);
      });

      socket.on('close', had_error => {
        reject(new RequestError('Socket closed with error'));
      });

    });

    request.on('response', response => {
      if (!response.statusCode) {
        reject(new RequestError('Request has invalid status code'));
      }
      response.socket.on('error', err => {
        debug('response socket error: ' + err.stack || err);
      });
      resolve(response);
    });

    request.on('abort', () => {
      reject(new RequestError('Request aborted by client (us)'));
    });

    request.on('aborted', () => {
      reject(new RequestError('Request aborted by server (them)'));
    });

    request.on('timeout', () => {
      request.abort();
      reject(new RequestError('Request timeout occured'));
    });
    
    // Set all headers which are specified in the request
    for (let key of Object.keys(opts.headers)) {
      request.setHeader(key, opts.headers[key]);
      debug(`set header ${key} to value ${opts.headers[key]}`);
    }

    // If we have an input stream for the request, we should specify
    // that here.  
    if (opts.stream) {
      request.setHeader('content-type', opts.contentType);
      debug('piping input stream to request');
      opts.stream.pipe(request);

      opts.stream.on('end', () => {
        debug('input stream completed, ending request');
        request.end();
      });

      opts.stream.on('error', err => {
        debug('error on input stream, aborting request');
        request.abort();
      });
    } else if (opts.data) {
      request.setHeader('content-type', opts.contentType);
      request.setHeader('content-length', opts.data.length);
      debug('writing: ' + opts.data);
      request.write(opts.data);
      request.end();
    } else {
      request.end();
    }
  });
}

