#!/usr/bin/env node
require('source-map-support').install();

let debugModule = require('debug');
let debug = debugModule('cloud-proxy:main');
let base = {
  app: require('taskcluster-lib-app'),
  validator: require('schema-validator-publisher'),
  loader: require('taskcluster-lib-loader'),
};
let config = require('typed-env-config');
let path = require('path');
let _ = require('lodash');
let assert = require('assert');
let taskcluster = require('taskcluster-client');
let uuid = require('uuid');
let aws = require('aws-sdk-promise');
let CacheManager = require('./cache-manager').CacheManager;
let QueueManager = require('./queue-manager').QueueManager;
let initQueue = require('./queue-manager').initQueue;
let S3StorageProvider = require('./s3-storage-provider').S3StorageProvider;

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
function compilePatterns (patterns) {
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
    requires: ['cfg'],
    setup: ({cfg}) => {
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
      sqsCfg.logger = awsDebugLoggerBridge;
      return new aws.SQS(sqsCfg);
    },
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: ({process, profile, cfg}) => monitoring({
      project: 'cloud-mirror',
      credentials: cfg.taskcluster.credentials,
      mock: profile === 'test',
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
    requires: ['cfg', 'validator', 'redis', 'registeredCacheManagers', 'monitor'],
    setup: ({cfg, validator, redis, registeredCacheManagers, monitor}) => v1.setup(
      {
        context: {
          validator: validator,
          redis: redis,
          cacheManagers: registeredCacheManagers,
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

  // This is so we can do tests with different queues
  queueUrlFactory: {
    requires: ['cfg', 'sqs'],
    setup: async ({cfg, sqs}) => {
      return async function (name) {
        return initQueue(sqs, name);
      };
    },
  },

  queueUrl: {
    requires: ['cfg', 'queueUrlFactory', 'profile'],
    setup: async ({cfg, queueUrlFactory, profile}) => queueUrlFactory(`cloud-mirror-${profile}`),
  },

  queueFactory: {
    requires: ['cfg', 'sqs', 'queueUrl', 'cacheManagers', 'profile', 'monitor'],
    setup: async ({cfg, sqs, queueUrl, cacheManagers, profile, monitor}) => {
      return async () => {
        let m = monitor.prefix('queue');

        let handler = async (obj) => {
          assert(obj.id, 'must provide id in queue request');
          assert(typeof obj.id === 'string', 'id must be string');
          assert(obj.url, 'must provide url in queue request');
          assert(typeof obj.url === 'string', 'url must be string');

          let selectedCacheManagers = cacheManagers.filter(x => x.id === obj.id);
          await Promise.all(selectedCacheManagers.map(x => x.put(obj.url)));
        };

        let deadHandler = async (rawMsg) => {
          m.count('dead-letters', 1);
          // TODO: figure out how to access the approximate retry attempt number
          // and submit that as a message to see how many times a message was
          // attempted before being dead lettered
          m.reportError(`Put request failed to complete: ${JSON.stringify(rawMsg)}`);
        };

        let queue = new QueueManager({
          sqs: sqs,
          batchSize: cfg.app.sqsBatchSize,
          handler: handler,
          queueUrl: queueUrl.queueUrl,
          deadHandler: deadHandler,
          deadQueueUrl: queueUrl.deadQueueUrl,
        });

        return queue;
      };
    },
  },

  backend: {
    requires: ['cfg', 'queueFactory'],
    setup: async ({cfg, queueFactory}) => {
      let queues = [];
      for (let x = 0; x < cfg.backend.count; x++) { 
        let queue = await queueFactory();
        queues.push(queue);
        queue.start();
      }
      return queues;
    },
  },

  cacheManagers: {
    requires: ['cfg', 'redis', 'profile', 'monitor'],
    setup: async ({cfg, redis, profile, monitor}) => {

      let cacheManagers = [];
      let s3regions = cfg.backend.s3.regions.split(',');

      for (let region of s3regions) {
        let bucket = cfg.backend.s3.bucketBase + profile + '-' + region;

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

        // Create the actual S3 object.
        let s3 = new aws.S3(awsCfg);

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

        await storageProvider.init();

        let cacheManager = new CacheManager({
          allowedPatterns: compilePatterns(cfg.app.allowedPatterns),
          cacheTTL: cfg.backend.cacheTTL,
          redis: redis,
          ensureSSL: cfg.app.ensureSSL,
          storageProvider: storageProvider,
          redirectLimit: cfg.app.redirectLimit,
          monitor: monitor.prefix('cache-manager'),
        });

        await cacheManager.init();

        cacheManagers.push(cacheManager);
      }

      return cacheManagers;
    },
  },

  // This needs to be a split out module because we'd otherwise
  // have a depenency loop of qf -> cm -> qf...
  registeredCacheManagers: {
    requires: ['cacheManagers', 'queueFactory'],
    setup: async ({cacheManagers, queueFactory}) => {
      let queue = await queueFactory();
      for (let cm of cacheManagers) {
        cm.registerQueue(queue);
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

      async function x () {
        console.log('checking on sqs queue');
        let result = await sqs.getQueueAttributes({
          QueueUrl: queueUrl.queueUrl,
          AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
          ],
        }).promise();
        let messagesWaiting = parseInt(result.data.Attributes.ApproximateNumberOfMessages, 10);
        let messagesInProcessing = parseInt(result.data.Attributes.ApproximateNumberOfMessagesNotVisible, 10);

        m.measure('waiting', messagesWaiting);
        m.measure('in-flight', messagesInProcessing);
        console.log(`There are ${messagesWaiting} waiting and ${messagesInProcessing} in flight`);
        setTimeout(x, 10000);
      }

      setTimeout(x, 0);
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
