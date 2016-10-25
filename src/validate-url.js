let request = require('./request').request;
let debug = require('debug')('cloud-mirror:follow-redirects');
let assert = require('assert');
let url = require('url');

// Just for STATUS_CODES
let http = require('http');

// Validate a URL against a list of patterns.  If any pattern is matching, then
// it's considered valid.  This means that it is a whitelist and not a
// blacklist.  Checking for SSL is done in the src/request.js file instead of
// here to avoid duplicating that check
function validateUrl(u, allowedPatterns) {
  for (let pattern of allowedPatterns) {
    if (pattern.test(u)) {
      return true;
    }
  }
  return false;
}

// Follow a redirect and return the final URL, headers of the final resource
// and a list of addresses which were redirected to.  If `ensureSSL` is
// specified then all urls in the redirect chain *must* be HTTPS urls.
//
// This function either throws an exception in error cases and will return an
// object in the shape:
// { url: <finalUrl>,
//   headers: <headers of finalUrl>,
//   statusCode: <http status code of finalUrl>,
//   addresses: <list of e.g. {c: <statusCode>, u: <url>, t: <iso datetime>} }
async function followRedirect(opts) {
  assert(typeof opts === 'object');
  assert(typeof opts.url === 'string');
  assert(typeof opts.ensureSSL !== 'undefined');
  assert(Array.isArray(opts.allowedPatterns));
  let maxRedirects = 30;
  if (opts.maxRedirects) {
    assert(typeof opts.maxRedirects === 'number');
    maxRedirects = opts.maxRedirects;
  } 

  let u = opts.url;
  let addresses = [];

  for (let x = 0 ; x < maxRedirects ; x++) {
    if (!validateUrl(u, opts.allowedPatterns)) {
      let err = new Error('URL does not validate: ' + u);
      err.url = u;
      err.addresses = addresses;
      err.code = 'InvalidUrl';
      throw err;
    }

    let result = await request(u, {
      method: 'HEAD',
      allowUnsafeUrls: !opts.ensureSSL,
    });

    let code = result.statusCode;
    if (!http.STATUS_CODES[code.toString()]) {
      let err = new Error('Unexpected Status Code: ' + code + ' at ' + u);
      err.statusCode = code;
      err.addresses = addresses;
      err.url = u;
      err.code = 'UnexpectedStatus';
      throw err;
    }
  
    addresses.push({
      c: code,
      u: u,
      t: new Date().toISOString(),
    });

    if (code >= 200 && code < 300 || code === 304) {
      debug('found a good resource, returning');
      return {
        url: u,
        statusCode: code,
        headers: result.headers,
        addresses: addresses,
      };
    } else if (code >= 300 && code < 400 && code !== 305) {
      debug('found a redirect, redirecting');
      let locHead = result.headers.location;
      if (!locHead) {
        let err = new Error('Redirect missing location header');
        err.urlFrom = u;
        err.urlTo = locHead;
        err.code = 'RedirectMissingLocationHeader';
        throw err;
      }
      u = url.resolve(u, locHead);
    } else {
      debug('found bad status code');
      let err = new Error('Unexpected HTTP Status: ' + code);
      err.statusCode = code;
      err.headers = result.headers;
      err.code = 'BadHTTPStatus';
      throw err;
    }
  }

  // If we're getting here, it's a sign that we've hit the maximum number
  // of redirects.  Let's bail here.
  let err = new Error('Too many redirects');
  err.addresses = addresses;
  err.lastUrl = u;
  throw err;
}

module.exports = followRedirect;
