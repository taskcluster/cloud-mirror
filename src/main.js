#!/usr/bin/env node
"use strict";
let debug = require('debug')('cloud-proxy:main');
let base = require('taskcluster-base');
let config = require('typed-env-config');
let path = require('path');
let _ = require('lodash');
let assert = require('assert');
let taskcluster = require('taskcluster-client');
let Memcached = require('memcache-promise');
let aws = require('aws-sdk-promise');
let storageBackend = require('./storage-backend');

//let exchanges = require('./exchanges');
let v1 = require('./api-v1');

// Create component loader
let load = base.loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => config({profile}),
  },

  memcached: {
    requires: ['cfg'],
    setup: ({cfg}) => {
      assert(cfg.memcached.servers, 'Must specify memcached servers');
      return new Memcached(cfg.memcached.servers);
    },
  },

  sqs: {
    requires: ['cfg'],
    setup: ({cfg}) => {
      assert(cfg.sqs, 'Must specify config for SQS');
      return new aws.SQS(cfg.sqs);
    },
  },

  influx: {
    requires: ['cfg'],
    setup: ({cfg}) => {
      if (cfg.influx.connectionString) {
        return new base.stats.Influx(cfg.influx);
      }
      return new base.stats.NullDrain();
    }
  },

  monitor: {
    requires: ['cfg', 'influx', 'process'],
    setup: ({cfg, influx, process}) => base.stats.startProcessUsageReporting({
      drain:      influx,
      component:  cfg.app.statsComponent,
      process:    process
    })
  },

  // Validator and publisher
  validator: {
    requires: ['cfg'],
    setup: ({cfg}) => base.validator({
      folder:        path.join(__dirname, 'schemas'),
      constants:     require('./schemas/constants'),
      publish:       cfg.app.publishMetaData,
      schemaPrefix:  'cloud-mirror/v1/',
      aws:           cfg.aws
    })
  },

  /*publisher: {
    requires: ['cfg', 'validator', 'influx', 'process'],
    setup: ({cfg, validator, influx, process}) => exchanges.setup({
      credentials:        cfg.pulse,
      exchangePrefix:     cfg.app.exchangePrefix,
      validator:          validator,
      referencePrefix:    'cloud-mirror/v1/exchanges.json',
      publish:            cfg.app.publishMetaData,
      aws:                cfg.aws,
      drain:              influx,
      component:          cfg.app.statsComponent,
      process:            process
    })
  },*/

  api: {
    requires: [
      'cfg', /*'publisher',*/ 'validator', 'influx', 'memcached',
    ],
    setup: (ctx) => v1.setup({
      context: {
        //publisher: ctx.publisher,
        validator: ctx.validator,
        memcached: ctx.memcached,
      },
      validator: ctx.validator,
      authBaseUrl: ctx.cfg.taskcluster.authBaseUrl,
      publish: ctx.cfg.app.publishMetaData,
      baseUrl: ctx.cfg.server.publicUrl + '/v1',
      referencePrefix: 'cloud-mirror/v1/api.json',
      aws: ctx.cfg.aws,
      component: ctx.cfg.app.statsComponent,
      drain: ctx.influx
    })
  },

  // Create the server process
  server: {
    requires: ['cfg', 'api', 'monitor'],
    setup: ({cfg, api}) => {
      let app = base.app(cfg.server);
      app.use('/v1', api);
      return app.createServer();
    }
  },

  s3backends: {
    requires: ['cfg', 'sqs', 'memcached', 'profile'], 
    setup: async ({cfg, sqs, memcached, profile}) => {
      // This should probably not all be here...
      let awsBackends = [];
      let regions = cfg.backend.s3.regions.split(',');
      for (let region of regions) {
        let awsCfg = _.omit(cfg.aws, 'region');
        awsCfg.region = region;
        let s3 = new aws.S3(awsCfg);
        let bucket = cfg.backend.s3.bucketBase + profile || 'development';
        bucket += '-' + region;
        try {
          await s3.createBucket({
            Bucket: bucket,
            CreateBucketConfiguration: {
              LocationConstraint: region,
            },
            ACL: cfg.backend.s3.acl,
          });
        } catch (err) {
          if (err.code !== 'BucketAlreadyExists' &&
              err.code !== 'BucketAlreadyOwnedByYou') {
          throw err;
          }
          debug('S3 Bucket already exists');
        }
        let backend = new storageBackend.S3Backend({
          region: region,
          bucket: bucket,
          urlTTL: cfg.backend.memcachedTTL,
          sqs: sqs,
          s3: s3,
          memcached: memcached,
          allowedPatterns: [/.*/],
        });
        await backend.init();
        backend.startListeningToRequestQueue();
        awsBackends.push(backend);
      }
      return awsBackends;
    },
  },

}, ['profile', 'process']);

// If this file is executed launch component from first argument
if (!module.parent) {
  load(process.argv[2], {
    process: process.argv[2],
    profile: process.env.NODE_ENV,
  }).catch(err => {
    console.log(err.stack);
    process.exit(1);
  });
}

// Export load for tests
module.exports = load;
