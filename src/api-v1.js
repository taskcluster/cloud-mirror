let debug = require('debug')('cloud-mirror:api-v1');
let base = require('taskcluster-base');
let taskcluster = require('taskcluster-client');
let _ = require('lodash');

let GENERIC_ID_PATTERN = /^[a-zA-Z0-9-_]{1,22}$/;

let api = new base.API({
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
  title: "Redirect to a bucket in desired region", 
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
  if (error) {
    return res.status(400).json({
      msg: 'URL Must be URL encoded!',
    });
  }

  let logthingy = `${url} in ${service}/${region}`;
  debug(`Attempting to redirect to ${logthingy}`);

  if (service.toLowerCase() === 's3') {
    if (this.s3backends[region]) {
      let backend = this.s3backends[region];
      try {
        await backend.validateInputURL(url);
      } catch (err) {
        debug(err);
        debug(err.stack);
        // Intentionally vague because we don't want to reveal
        // our configuration too much
        let msg = 'Input URL failed validation for unknown reason';
        switch (err.code) {
          case 'HTTPError':
            msg = 'HTTP Error while trying to resolve redirects';
            break;
          case 'DoesNotMatchPatterns':
            msg = 'Input URL does not match whitelist';
            break;
          case 'InsecureURL':
            msg = 'Refusing to follow a non-SSL redirect';
            break;
        }
        return res.status(400).json({
          msg: msg,
        });
      }
      let result = await backend.getBackendUrl(url);
      if (result.status === 'present') {
        debug(`${logthingy} is present`);
        res.status(302);
        res.location(result.url);
        // Instead of just returning result object, we want to return only
        // known properties.  This is to avoid possible leakage
        return res.json({
          status: result.status,
          url: result.url,
        });
      } else if (result.status === 'pending') {
        debug(`${logthingy} is pending`);
        res.status(202);
        return res.json({
          status: result.status,
          futureUrl: result.url,
          noteOnFutureUrl: 'future url is not authoritatively where the' +
                           ' resource will be located.  Do not rely on' + 
                           ' this information!',
        });
      } else if (result.status === 'error') {
        debug(`${logthingy} is in error state`);
        res.status(400);
        return res.json({
          msg: 'Backend was unable to cache this item',
        });
      } else {
        throw new Error('Huh, should not be able to get here!');
      }
    } else {
      debug(`Region not configured for ${logthingy}`);
      return res.status(400).json({
        msg: `Region '${region}' is not configured for ${service}`,
      });
    }
  } else {
      debug(`Service not known for ${logthingy}`);
    return res.status(400).json({
      msg: `Service '${service}' is not known`,
    });
  }
});

api.declare({
  method: 'delete',
  route: '/expire/:service/:region/:url/:error?',
  name: 'expire',
  title: 'Expire resource',
  description: [
    'Documented later...',
  ].join('\n'),
}, async function (req, res) {
  let url = req.params.url;
  let region = req.params.region;
  let service = req.params.service;
  let error = req.params.error;
  if (error) {
    return res.status(400).json({
      msg: 'URL Must be URL encoded!',
    });
  }
  let logthingy = `${url} in ${service}/${region}`;
  debug(`Attempting to expire ${logthingy}`);

  if (service.toLowerCase() === 's3') {
    if (this.s3backends[region]) {
      let backend = this.s3backends[region];
      let result = await backend.expire(url);
      return res.status(204).send();
    } else {
      debug(`Region not configured for ${logthingy}`);
      return res.status(400).json({
        msg: `Region '${region}' is not configured for ${service}`,
      });
    }
  } else {
      debug(`Service not known for ${logthingy}`);
    return res.status(400).json({
      msg: `Service '${service}' is not known`,
    });
  }
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
