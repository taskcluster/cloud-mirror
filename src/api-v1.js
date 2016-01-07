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
  route: '/redirect/:service/:region/:url',
  name: 'redirect', 
  //deferAuth: false,
  //scopes: [],
  title: "Redirect to a bucket in desired region", 
  description: [
    'Redirect to the copy of :url in :region.  If there is',
    'no copy of the file in that region, submit a request to',
    'backend process to copy into that region and wait to respond',
    'here until that happens',
  ].join('\n'),
}, async function (req, res) {
  let url = req.params.url;
  let region = req.params.region;
  let service = req.params.service;
  debug('Would copy %s into %s', url, region);
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
