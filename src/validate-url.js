let request = require('request-promise').defaults({
  followRedirect: false,
  simple: false,
  resolveWithFullResponse: true,
});
let debug = require('debug')('cloud-mirror:follow-redirects');
let assert = require('assert');
let url = require('url');

/**
 * Follow redirects in a secure way, unless configured not to.  This function
 * will ensure that all redirects in a redirect chain are pointing to HTTPS
 * urls and not HTTP urls.  This is used to ensure that everything in the Cloud
 * Mirror storage was obtained through with an HTTP chain of custody.  If the
 * config parameter 'allowInsecureRedirect' is a truthy value, HTTP urls and
 * redirections from HTTPS to HTTP resources will be allowed.
 * 
 * This will return `false` if the url is invalid, an object with information if
 * it is valid and will throw if an error occured while doing the redirects
 */
async function validateUrl (firstUrl, allowedPatterns = [/.*/], redirectLimit = 30, ensureSSL = true) {
  // Number of redirects to follow
  let addresses = [];

  assert(firstUrl, 'Must provide URL to check');

  // What we're saying here is that all URLs passed through the redirector must
  // start with https: in order to avoid causing this function to throw
  let checkSSL = (u) => url.parse(u).protocol === 'https:';

  // If we aren't enforcing secure redirects, it's just easier to make the
  // validation function a no-op
  if (!ensureSSL) {
    checkSSL = () => true;
  }

  let patterns = allowedPatterns;
  // Now, let's check that the patterns we've given this function are valid
  let checkPatterns = (u) => {
    let valid = false;
    for (let p of patterns) {
      if (p.test(u)) {
        valid = true;
        break;
      }
    }
    return valid;
  };

  // the u variable points to the URL in the current redirect chain
  let u = firstUrl;

  // the c variable is short for continue and is used to decide whether
  // to continue following redirects
  let c = true;

  for (let i = 0 ; c && i < redirectLimit ; i++) {
    if (!checkSSL(u) || !checkPatterns(u)) {
      return false;
    }

    let result = await request.head(u);
    let sc = result.statusCode;

    // We store the chain of URLs that make up this redirection.  This could be
    // used if we wished to provide an audit trail in consuming systems
    addresses.push({
      code: sc,
      url: u,
      t: new Date(),
    });

    if (sc >= 200 && sc < 300) {
      // A 200 series redirect means that we're done.  We will not follow a 200
      // URL that has a Location: header because that's not part of the spec.
      // We can easily change this bit of code to make the choice of redirecting or
      // not based on the presence of the Location: header later if we choose.
      debug(`Follwed all redirects: ${addresses.map(x => x.url).join(' --> ')}`);
      return {
        url: u,
        meta: result,
        addresses,
      };
    } else if (sc >= 300 && sc < 400 && sc !== 304 && sc !== 305) {
      // 304 and 305 are not redirects
      assert(result.headers.location);
      let newU = result.headers.location;
      newU = url.resolve(u, newU);
      u = url.resolve(u, newU);
    } else {
      // Consider using an exponential backoff and retry here
      let err = new Error('HTTP Error while redirecting');
      err.code = 'HTTPError';
      throw err;
    }
  }

  debug(`Limit of ${redirectLimit} redirects reached:` + addresses.map(x => x.url).join(' --> '));
  return false;
};

module.exports = validateUrl;
