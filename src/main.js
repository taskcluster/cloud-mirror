#!/usr/bin/env node
if (process.versions.node.split('.')[0] < 5) {
  throw new Error('Only Node 5+ is supported');
}
require('source-map-support').install();

let debugModule = require('debug');
let debug = debugModule('cloud-proxy:main');
let base = {
  app: require('taskcluster-lib-app'),
  validator: require('taskcluster-lib-validate'),
  loader: require('taskcluster-lib-loader'),
};
let config = require('typed-env-config');
let path = require('path');
let _ = require('lodash');
let assert = require('assert');
let taskcluster = require('taskcluster-client');
let uuid = require('uuid');
let aws = require('aws-sdk');
let CacheManager = require('./cache-manager').CacheManager;
let S3StorageProvider = require('./s3-storage-provider').S3StorageProvider;
let createS3Bucket = require('./s3-storage-provider').createS3Bucket;
let sqsSimple = require('sqs-simple');

let bluebird = require('bluebird');
let redis = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

//let exchanges = require('./exchanges');
let v1 = require('./api-v1');

let monitoring = require('taskcluster-lib-monitor');

let testBucket;

/**
 * Take a list of string regular expressions, verify that they meet conditions
 * that we've established as valid for allowed patterns and return a list of
 * regular expression objects
 */
function compilePatterns(patterns) {
  let regexps = [];
  for (let pattern of patterns) {
    if (!pattern.startsWith('^')) {
      throw new Error('All allowed patterns must start with ^ character');
    }

    if (!pattern.endsWith('/')) {
      throw new Error('All allowed patterns must end with / character');
    }

    regexps.push(new RegExp(pattern));
  }

  return regexps;
}

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
    requires: ['cfg', 'monitor'],
    setup: ({cfg, monitor}) => {
      assert(cfg.sqs, 'Must specify config for SQS');
      let sqsCfg = cfg.sqs;
      let sqsDebugger = debugModule('cloud-mirror:aws-sqs');
      let awsDebugLoggerBridge = {
        write: x => {
          for (let y of x.split('\n')) {
            sqsDebugger(y);
          }
        },
      };
      //sqsCfg.logger = awsDebugLoggerBridge;
      let sqs = new aws.SQS(sqsCfg);
      monitor.patchAWS(sqs);
      return sqs;
    },
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: ({process, profile, cfg}) => monitoring({
      project: profile === 'production ? 'cloud-mirror' : 'cloud-mirror-us-east-1',
      credentials: cfg.taskcluster.credentials,
      mock: /^production/.test(profile),
      process,
    }),
  },

  validator: {
    requires: ['cfg'],
    setup: ({cfg}) => base.validator(
      {
        folder:        path.join(__dirname, 'schemas'),
        constants:     require('./schemas/constants'),
        publish:       cfg.app.publishMetaData,
        prefix:  'cloud-mirror/v1/',
        aws:           cfg.aws,
      }
    ),
  },

  api: {
    requires: ['cfg', 'validator', 'redis', 'cacheManagers', 'monitor'],
    setup: ({cfg, validator, redis, cacheManagers, monitor}) => v1.setup(
      {
        context: {
          validator: validator,
          redis: redis,
          cacheManagers: cacheManagers,
          maxWaitForCachedCopy: cfg.app.maxWaitForCachedCopy,
          allowedPatterns: compilePatterns(cfg.app.allowedPatterns),
          redirectLimit: cfg.app.redirectLimit,
          ensureSSL: cfg.app.ensureSSL,
          monitor: monitor.prefix('api'),
        },
        validator: validator,
        authBaseUrl: cfg.taskcluster.authBaseUrl,
        publish: cfg.app.publishMetaData,
        baseUrl: cfg.server.publicUrl + '/v1',
        referencePrefix: 'cloud-mirror/v1/api.json',
        aws: cfg.aws,
        monitor: monitor.prefix('api'),
      },
    ),
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

  initQueue: {
    requires: ['cfg', 'sqs'],
    setup: async ({cfg, sqs}) => {
      let sqsConfig = cfg.sqsSimple;
      sqsConfig.sqs = sqs;
      debug(sqsConfig);
      return sqsSimple.initQueue(sqsConfig);
    },
  },

  queueUrl: {
    requires: ['cfg', 'sqs'],
    setup: async ({cfg, sqs}) => {
      return sqsSimple.getQueueUrl({
        sqs,
        queueName: cfg.sqsSimple.queueName,
      });
    },
  },

  deadQueueUrl: {
    requires: ['cfg', 'sqs', 'profile'],
    setup: async ({cfg, sqs, profile}) => {
      return sqsSimple.getQueueUrl({
        sqs,
        queueName: cfg.sqsSimple.queueName + cfg.sqsSimple.deadLetterSuffix,
      });
    },
  },

  queueSender: {
    requires: ['cfg', 'sqs', 'queueUrl'],
    setup: async ({cfg, sqs, queueUrl}) => {
      return new sqsSimple.QueueSender({sqs, queueUrl});
    },
  },

  queueListenerFactory: {
    requires: ['cfg', 'sqs', 'queueUrl', 'cacheManagers', 'profile', 'monitor'],
    setup: async ({cfg, sqs, queueUrl, cacheManagers, profile, monitor}) => {
      return async function() {
        let listenerOpts = _.pick(cfg.sqsSimple, ['maxReceiveCount', 'visibilityTimeout', 'deadLetterSuffix']);

        listenerOpts.queueUrl = queueUrl;
        listenerOpts.sqs = sqs;

        listenerOpts.handler = async (msg, changeTimeout) => {
          debug('received message!');
          assert(typeof msg.id === 'string', 'id must be string');
          assert(typeof msg.url === 'string', 'url must be string');
          debug('this message checks out');

          let selectedCacheManagers = cacheManagers.filter(x => x.id === msg.id);

          await Promise.all(selectedCacheManagers.map(x => x.put(msg.url)));
        };
        
        let listener = new sqsSimple.QueueListener(listenerOpts);

        listener.on('error', (err, errType) => {
          let level = errType === 'payload' ? 'debug' : 'warning';
          monitor.reportError(err, level, {type: errType});
          debug('%s %s', err.stack || err, errType);
          if (errType === 'api') {
            console.log('Encountered an API error, exiting');
            process.exit(1);
          }
        });

        return listener;
      };
    },
  },

  deadQueueListener: {
    requires: ['cfg', 'sqs', 'deadQueueUrl', 'monitor'],
    setup: async ({cfg, sqs, deadQueueUrl, monitor}) => {
      let listenerOpts = _.pick(cfg.sqsSimple, ['maxReceiveCount', 'visibilityTimeout', 'deadLetterSuffix']);

      listenerOpts.queueUrl = deadQueueUrl;
      listenerOpts.sqs = sqs;

      listenerOpts.handler = async (msg, changeTimeout) => {
        monitor.count('dead-letters', 1);
        let err = new Error('dead-letter');
        err.originalMessage = msg;
        monitor.reportError(err, 'info');
      };

      let listener = new sqsSimple.QueueListener(listenerOpts);

      listener.on('error', (err, errType) => {
        let level = errType === 'payload' ? 'debug' : 'warning';
        monitor.reportError(err, level, {type: errType});
        debug('%s %s', err.stack || err, errType);
      });

      return listener;
    },
  },

  backend: {
    requires: ['cfg', 'queueListenerFactory', 'deadQueueListener'],
    setup: async ({cfg, queueListenerFactory, deadQueueListener}) => {
      let queues = [];
      for (let x = 0; x < cfg.backend.count; x++) { 
        let queue = await queueListenerFactory();
        queues.push(queue);
        queue.start();
      }

      deadQueueListener.start();
      return queues;
    },
  },

  /*
   * Rather than duplicating this logic in a couple different places,
   * let's create a factory that can give us properly configured
   * S3 objects given a region
   */
  s3Factory: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      return function(region) {
        // We want to log aws-sdk calls so we write a custom file like object
        // which uses a debug function instead of the default of writing
        // directly to stdout
        let awsCfg = _.omit(cfg.aws, 'region');

        awsCfg.region = region;

        let s3Debugger = debugModule('cloud-mirror:aws-s3:' + region);

        let awsDebugLoggerBridge = {
          write: x => {
            for (let y of x.split('\n')) {
              s3Debugger(y);
            }
          },
        };

        awsCfg.logger = awsDebugLoggerBridge;

        return new aws.S3(awsCfg);      
      };
    },
  },

  s3buckets: {
    requires: ['cfg', 'profile', 'monitor', 's3Factory'],
    setup: async ({cfg, profile, monitor, s3Factory}) => {
      let s3Regions = cfg.backend.s3.regions.split(',');
      await Promise.all(s3Regions.map(async region => {
        let bucket = cfg.backend.s3.bucketBase + profile + '-' + region;
        let acl = cfg.backend.s3.acl;
        let lifespan = cfg.backend.s3.lifespan;
        let s3 = await s3Factory(region);
        await createS3Bucket(s3, bucket, region, acl, lifespan);
        console.log('Finished %s', region);
      }));
      console.log('Finished all regions');
    },
  },

  cacheManagers: {
    requires: ['cfg', 'redis', 'profile', 'queueSender', 'monitor', 's3Factory'],
    setup: async ({cfg, redis, profile, queueSender, monitor, s3Factory}) => {

      let cacheManagers = [];
      let s3regions = cfg.backend.s3.regions.split(',');

      for (let region of s3regions) {
        let bucket = cfg.backend.s3.bucketBase + profile + '-' + region;

        // Create the actual S3 object.
        let s3 = await s3Factory(region);
        monitor.patchAWS(s3);

        let storageProvider = new S3StorageProvider({
          service: 's3',
          region: region,
          bucket: bucket,
          partSize: cfg.backend.s3.partSize,
          queueSize: cfg.backend.s3.queueSize,
          s3: s3,
          acl: cfg.backend.s3.acl,
          lifespan: cfg.backend.s3.lifespan,
          monitor: monitor.prefix(`s3-${region}`),
        });

        let cacheManager = new CacheManager({
          allowedPatterns: compilePatterns(cfg.app.allowedPatterns),
          cacheTTL: cfg.backend.cacheTTL,
          redis: redis,
          ensureSSL: cfg.app.ensureSSL,
          queueSender: queueSender,
          storageProvider: storageProvider,
          redirectLimit: cfg.app.redirectLimit,
          monitor: monitor.prefix('cache-manager'),
        });

        cacheManagers.push(cacheManager);
      }

      return cacheManagers;
    },
  },

  // We need to be able to monitor how many messages live in the queue.  This
  // is not intended to be long living code and so has a bunch of things
  // hardcoded in.  If you'd like, feel free to put this into config.yml
  queueMonitor: {
    requires: ['monitor', 'sqs', 'profile', 'queueUrl'],
    setup: async ({monitor, sqs, profile, queueUrl}) => {
      let m = monitor.prefix(`cloud-mirror.${profile}.sqs-messages`);

      while (true) {
        console.log('checking on sqs queue');
        let result = await sqs.getQueueAttributes({
          QueueUrl: queueUrl,
          AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
          ],
        }).promise();

        let messagesWaiting = parseInt(result.Attributes.ApproximateNumberOfMessages, 10);
        let messagesInProcessing = parseInt(result.Attributes.ApproximateNumberOfMessagesNotVisible, 10);

        m.measure('visible', messagesWaiting);
        m.measure('in-flight', messagesInProcessing);
        console.log(`There are ${messagesWaiting} waiting and ${messagesInProcessing} in flight`);
        await new Promise(accept => setTimeout(accept, 2000));
      }
    },
  },

  all: {
    requires: ['backend', 'server'],
    setup: async ({backend, server}) => {
      await Promise.race([backend, server]);
    },
  },

}, ['profile', 'process']);

// If this file is executed launch component from first argument
if (!module.parent) {
  require('source-map-support').install();
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
