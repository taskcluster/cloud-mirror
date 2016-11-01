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
 * Returns a cleaned up version of the HTTP headers, after ensuring they
 * are in the correct format.  Will throw an exception if there are collisions
 * caused by case issues of header names
 */
function correctHeaders(headers) {
  if (typeof headers !== 'object') {
    return false;
  }

  let newHeaders = {};

  for (let key of Object.keys(headers)) {
    if (typeof headers[key] !== 'string') {
      throw new RequestError('Header value for ' + key + ' must be string');
    }
    if (newHeaders[key.toLowerCase()]) {
      if (newHeaders[key.toLowerCase()] !== headers[key]) {
        throw new RequestError('Header ' + key + ' collision caused by casing and not same value'); 
      }
    }
    newHeaders[key.toLowerCase()] = headers[key];
  }
  return newHeaders;
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

  // We want the protocol, port and path of the URL.
  let {protocol, hostname, port, path} = urllib.parse(url);

  // Since we don't know how to interpret anything other than these protocols,
  // we should error if something else is given
  if (protocol !== 'https:' && protocol !== 'http:') {
    throw new RequestError('Only HTTP and HTTPS urls are supported');
  }

  // We want to ensure that by default we only use HTTPS, and this is one of
  // the ways we do this.
  if (!opts.allowUnsafeUrls) {
    assert(protocol === 'https:', 'only https is supported');
  } else {
    debug('WARNING: allowing unsafe urls');
  }

  // We want to validate all the headers
  let headers = correctHeaders(opts.headers || {});
  
  // We want to make the User Agent string impossible to override
  headers['user-agent'] = 'cloud-mirror' + versionString;

  // We want to parse the port because urllib.parse will return 
  // its string base10 representation
  if (port && typeof port === 'string') {
    port = parseInt(port, 10);
    if (Number.isNaN(port)) {
      throw new RequestError('Provided port is not parsable');
    }
  }

  // Set default ports
  if (!port && protocol === 'https:') {
    port = 443;
  } else if (!port && protocol === 'http:') {
    port = 80;
  }

  // Let's save a headache and use upper case letters for the Method, always
  let method = opts.method || 'GET';
  method = method.toUpperCase();

  // We want to make sure that the method used is one known to the HTTP
  // library.  Since HTTPS is just HTTP running over TLS sockets, we use the
  // same list of methods for https
  assert(http.METHODS.indexOf(method) !== -1);
  
  // Handle input body streams
  if (opts.stream) {
    assert(typeof opts.contentType === 'string',
        'when supplying a request body, you must specify content-type');
    assert(!opts.data, 'if supplying a stream, you must not supply data');
  }

  // Handle input body blobs
  if (opts.data) {
    assert(typeof opts.contentType === 'string',
        'when supplying a request body, you must specify content-type');
    assert(!opts.stream, 'if supplying a stream, you must not supply data');
  }

  return makeRequest({
    httpLib: protocol === 'https:' ? https : http,
    hostname: hostname,
    port: port,
    path: path,
    headers: headers,
    method: method,
    stream: opts.stream,
    data: opts.data,
    contentType: opts.contentType,
    timeout: opts.timeout || 60000,
  });
}

module.exports = {
  request,
  RequestError,
  correctHeaders,
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
      timeout: opts.timeout,
      headers: opts.headers,
    });

    request.on('error', reject);

    request.on('socket', socket => {
      debug('have a socket');

      socket.on('error', reject);

      socket.on('close', had_error => {
        if (had_error) {
          reject(new RequestError('Socket closed with error'));
        }
      });

    });

    request.on('response', resolve);

    request.on('abort', () => {
      reject(new RequestError('Request aborted by client (us)'));
    });

    request.on('aborted', () => {
      reject(new RequestError('Request aborted by server (them)'));
    });

    request.on('timeout', () => {
      request.abort();
    });
    
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

