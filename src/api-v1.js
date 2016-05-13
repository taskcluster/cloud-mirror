let debug = require('debug')('cloud-mirror:api-v1');
let API = require('taskcluster-lib-api');
let taskcluster = require('taskcluster-client');
let _ = require('lodash');
let delayer = require('./delayer');
let validateUrl = require('./validate-url');

let GENERIC_ID_PATTERN = /^[a-zA-Z0-9-_]{1,22}$/;

let api = new API({
  title: 'Cloud Mirror API',
  description: 'Service to duplicate URLs from various cloud providers',
  schemaPrefix: 'http://schemas.taskcluster.net/s3-distribute/v1/',
  params: {
    taskId: GENERIC_ID_PATTERN,
    runId: GENERIC_ID_PATTERN,
    name: GENERIC_ID_PATTERN,
  },
});

module.exports = api;

api.declare({
  method: 'get',
  // Note that the Error parameter is only here to check for improperly
  // URL-Encoded URLs.  This would likely cause errors if this API were
  // included in the taskcluster-client* packages.  Maybe we should have a
  // parameter to api.declare that lets us ignore certain endpoints from
  // generated API-References.  A value of '', e.g "/redirect/s/r/u/" would be
  // fine, it just has to evaluate as falsy
  route: '/redirect/:service/:region/:url/:error?',
  name: 'redirect',
  //deferAuth: false,
  //scopes: [],
  title: 'Redirect to backing cache',
  description: [
    'Redirect to the copy of :url in :region.  If there is',
    'no copy of the file in that region, submit a request to',
    'backend process to copy into that region and wait to respond',
    'here until that happens',
    '',
    'NOTE: URL parameter must be URL Encoded!',
    '',
    'NOTE: If using this with an api-reference consuming client',
    'you will need to pass error as an empty string!',
  ].join('\n'),
}, async function (req, res) {
  let url = req.params.url;
  let region = req.params.region;
  let service = req.params.service;
  let error = req.params.error;

  // Because of how URLs work, its not possible to have a route description
  // which captures the http:// as a single parameter.  Instead, we have a
  // parameter after where the urlencoded url ought to be.  If this is any
  // js-truthy value, we know that someone didn't pass in url-encoded url.
  if (error) {
    return res.reportError(
        'InputError',
        'URL Must be URL Encoded!',
        {url, error}
    );
  }

  let logthingy = `${url} in ${service}/${region}`;

  // This is the ID that we need to find a backend for
  let incomingId = `${service}_${region}`;
  
  // Let's pick which backend to use.  In this case, we're looking to find the
  // only backend known that matches the potential id
  let backends = this.cacheManagers.filter(x => x.id === incomingId);

  if (backends.length > 1) {
    debug('[alert-operator] API server is misconfigured and has more than one cachemanager with id, crashing' + incomingId);
    // Because this should never ever happen
    process.exit(-1);
  } else if (backends.length === 0) {
    debug(`${incomingId} is not known`);
    return res.reportError(
        'ResourceNotFound',
        'service or region not found',
        {url, region, service}
    );
  } else {
    let backend = backends[0];
    let maxWait = this.maxWaitForCachedCopy;
    let startTime = new Date();
    let x = 0;
    let result;

    do {
      result = await backend.getUrlForRedirect(url);
      // We only want to do validation a single time.  Since we need to use a value
      // that's fetched inside the do-while-loop, I decided to check for the first iteration
      // instead of a more complicated structure
      if (x === 0 && (result.status === 'absent' || result.status === 'error')) {
        let validUrl = await validateUrl(url, this.allowedPatterns, this.redirectLimit, this.ensureSSL);

        if (!validUrl) {
          return res.status(403).json({
            msg: 'URL is not allowed',
            url: url,
          });
        }
      }

      if (result.status === 'present') {
        debug(`${logthingy} is present`);
        return res.status(302).location(result.url).json({
          status: result.status,
          url: result.url,
        });
      } else if (result.status === 'pending') {
        // Do nothing for pending operations
      } else if (result.status === 'absent') {
        debug(`${logthingy} is absent, requesting`);
        backend.requestPut(url);
      } else if (result.status === 'error') {
        debug(`${logthingy} had error, retrying request.  Backend stack: ${result.stack}`);
        backend.requestPut(url);
      } else {
        debug(`[alert-opereator] ${logthingy} invalid status: ${result.status}`);
      }
      await delayer(1000);
    } while (new Date() - startTime < maxWait);

    // If we get here, we're doing the fallback of redirecting
    // to the original URL because the caching took too long
    debug(`[alert-operator] Redirecting to uncached copy because it took too long ${url}`);

    return res.status(302).location(url).json({
      url: url,
      msg: `Cached copy did not show up in ${maxWait/1000}s`,
    });
  }
});

api.declare({
  method: 'delete',
  // Note that the Error parameter is only here to check for improperly
  // URL-Encoded URLs.  This would likely cause errors if this API were
  // included in the taskcluster-client* packages.  Maybe we should have a
  // parameter to api.declare that lets us ignore certain endpoints from
  // generated API-References.  A value of '', e.g "/redirect/s/r/u/" would be
  // fine, it just has to evaluate as falsy
  route: '/purge/:service/:region/:url/:error?',
  name: 'purge',
  //deferAuth: false,
  //scopes: [],
  title: 'Purge resource from backing cache',
  description: [
    'Redirect to the copy of :url in :region.  If there is',
    'no copy of the file in that region, submit a request to',
    'backend process to copy into that region and wait to respond',
    'here until that happens',
    '',
    'NOTE: URL parameter must be URL Encoded!',
    '',
    'NOTE: If using this with an api-reference consuming client',
    'you will need to pass error as an empty string!',
  ].join('\n'),
}, async function (req, res) {
  let url = req.params.url;
  let region = req.params.region;
  let service = req.params.service;
  let error = req.params.error;

  // See comment in the redirect message to explain this parameter
  if (error) {
    return res.reportError(
        'InputError',
        'URL Must be URL Encoded!',
        {url, error}
    );
  }

  let logthingy = `${url} in ${service}/${region}`;
  debug(`Attempting to purge ${logthingy}`);
  // This is the ID that we need to find a backend for
  let incomingId = `${service}-${region}`;
  
  // Let's pick which backend to use.  In this case, we're looking to find the
  // only backend known that matches the potential id
  let backends = this.cacheManagers.filter(x => x.id === incomingId);
  if (backends.length > 1) {
    throw new Error('API server is misconfigured and has more than one cachemanager with id, crashing' + incomingId);
  } else if (backends.length === 0) {
    debug(`${incomingId} is not known`);
    return res.reportError(
        'ResourceNotFound',
        'service or region not found',
        {url, region, service}
    );
  }

  let backend = backends[0];
  await backend.purge(url);
  return res.status(204).send();
});

api.declare({
  method: 'get',
  route: '/ping',
  name: 'ping',
  title: 'Ping Server',
  description: [
    'Documented later...',
    '',
    '**Warning** this api end-point is **not stable**.',
  ].join('\n'),
}, function (req, res) {
  res.status(200).json({
    alive: true,
    uptime: process.uptime(),
  });
});

api.declare({
  method: 'get',
  route: '/api-reference',
  name: 'apiReference',
  title: 'api reference',
  description: [
    'Get an API reference!',
    '',
    '**Warning** this api end-point is **not stable**.',
  ].join('\n'),
}, function (req, res) {
  let host = req.get('host');
  let proto = req.connection.encrypted ? 'https' : 'http';
  res.status(200).json(api.reference({
    baseUrl: proto + '://' + host + '/v1',
  }));
});
