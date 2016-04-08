#!/usr/bin/env node
let debugModule = require('debug');
let debug = debugModule('cloud-proxy:main');
let base = require('taskcluster-base');
let config = require('typed-env-config');
let path = require('path');
let _ = require('lodash');
let assert = require('assert');
let taskcluster = require('taskcluster-client');
let uuid = require('uuid');

let aws = require('aws-sdk-promise');
let CacheManager = require('./cache-manager').CacheManager;
let S3StorageProvider = require('./s3-storage-provider').S3StorageProvider;

let bluebird = require('bluebird');
let redis = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

//let exchanges = require('./exchanges');
let v1 = require('./api-v1');

let testBucket;

// Create component loader
let load = base.loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => config({profile}),
  },

  redis: {
    requires: ['cfg'],
    setup: ({cfg}) => {
      assert(cfg.redis, 'Must specify redis server');
      debug('Redis config: %j', cfg.redis);
      return redis.createClient(cfg.redis);
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
      'cfg', /*'publisher',*/ 'validator', 'influx', 'redis', 's3backends',
    ],
    setup: (ctx) => v1.setup({
      context: {
        //publisher: ctx.publisher,
        validator: ctx.validator,
        redis: ctx.redis,
        s3backends: ctx.s3backends,
        maxWaitForCachedCopy: ctx.cfg.app.maxWaitForCachedCopy,
        allowedPatterns: ctx.cfg.app.allowedPatterns.map(x => new RegExp(x)),
        redirectLimit: ctx.cfg.app.redirectLimit,
        ensureSSL: ctx.cfg.app.ensureSSL,
      },
      validator: ctx.validator,
      authBaseUrl: ctx.cfg.taskcluster.authBaseUrl,
      publish: ctx.cfg.app.publishMetaData,
      baseUrl: ctx.cfg.server.publicUrl + '/v1',
      referencePrefix: 'cloud-mirror/v1/api.json',
      aws: ctx.cfg.aws,
      component: ctx.cfg.app.statsComponent,
      drain: ctx.influx,
    })
  },

  // Create the server process
  server: {
    requires: ['cfg', 'api', 'monitor'],
    setup: ({cfg, api}) => {
      let app = base.app(cfg.server);
      app.use('/v1', api);
      return app.createServer();
    },
  },

  s3backends: {
    requires: ['cfg', 'sqs', 'redis', 'profile'],
    setup: async ({cfg, sqs, redis, profile}) => {

      let cacheManagers = {};
      let regions = cfg.backend.s3.regions.split(',');

      for (let region of regions) {
        let bucket;
        if (profile === 'test') {
          if (!testBucket) {
            testBucket = uuid.v4().replace(/-/g, '');
            // Prefix for easy cleanup
            testBucket = 'cloud-mirror-test-' + testBucket;
          }
          bucket = testBucket;
        } else {
          bucket = cfg.backend.s3.bucketBase + cfg.server.env + '-' + region;
        }

        let awsCfg = _.omit(cfg.aws, 'region');
        awsCfg.region = region;
        let s3Debugger = debugModule('cloud-mirror:aws-s3:' + region);
        let awsDebugLoggerBridge = {
          write: x => {
            for (let y of x.split('\n')) {
              s3Debugger(y);
            }
          }
        };
        awsCfg.logger = awsDebugLoggerBridge;
        let s3 = new aws.S3(awsCfg);
        let storageProvider = new S3StorageProvider({
          region: region,
          bucket: bucket,
          partSize: cfg.backend.s3.partSize,
          queueSize: cfg.backend.s3.queueSize,
          s3: s3,
          acl: cfg.backend.s3.acl,
          lifespan: cfg.backend.s3.lifespan,
        });

        await storageProvider.init();

        let cacheManager = new CacheManager({
          putQueueName: `S3-${region}-${profile}-put-request`,
          allowedPatterns: cfg.app.allowedPatterns.map(x => new RegExp(x)),
          cacheTTL: cfg.backend.cacheTTL,
          redis: redis,
          sqs: sqs,
          ensureSSL: cfg.app.ensureSSL,
          storageProvider: storageProvider,
          redirectLimit: cfg.app.redirectLimit,
          sqsBatchSize: cfg.app.sqsBatchSize,
        });

        await cacheManager.init();

        cacheManagers[region] = cacheManager;
      }

      return cacheManagers;
    },
  },

  listeningS3Backends: {
    requires: ['cfg', 's3backends', 'monitor'],
    setup: async ({s3backends}) => {
      for (let region of _.keys(s3backends)) {
        s3backends[region].startListeningToPutQueue();
      }
      return s3backends;
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
