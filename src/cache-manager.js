let debug = require('debug')('cloud-mirror:cache-manager');
let urllib = require('url');
let http = require('http');
let requestPromise = require('request-promise').defaults({
  followRedirect: false,
  simple: false,
  resolveWithFullResponse: true
});
let request = require('request').defaults({
  followRedirect: false,
});
let fs = require('fs');
let stream = require('stream');
let meter = require('stream-meter');
let SQSConsumer = require('sqs-consumer');
let debugModule = require('debug');
let followRedirects = require('./follow-redirects');
let _ = require('lodash');
let assert = require('assert');

const CACHE_STATES = ['present', 'pending', 'error'];

class CacheManager {
  constructor(config) {
    for (let x of [
      'putQueueName', // SQS queue to listen on for copy requests
      'allowedPatterns', // Regular expressions to validate input urls
      'cacheTTL', // Number of seconds to keep URL in the cache
      'redis', // Redis object where we should cache metadata
      'sqs', // SQS Instance to listen and publish on
      'ensureSSL', // true if we should force only HTTP in redirect links
      'storageProvider', // StorageProvider instance to manage
    ]) {
      assert(typeof config[x] !== 'undefined', `CacheManager requires ${x} configuration value`);
      this[x] = config[x];
    }

    // Maximum number of redirects to follow
    this.redirectLimit = config.redirectLimit || 30;

    // Maximum number of concurrent messages to process
    this.sqsBatchSize = config.sqsBatchSize || 10;

    // We'll use the same ID here as we have set in the storage provider
    this.id = this.storageProvider.id;
    this.debug = debugModule(`cloud-mirror:${this.constructor.name}:${this.id}`);

    this.putQueueNameDead = this.putQueueName + '_dead';
  }

  async init() {
    // First, we'll create the dead letter queue
    let awsRes = await this.sqs.createQueue({
      QueueName: this.putQueueNameDead,
    }).promise();
    this.putQueueDeadUrl = awsRes.data.QueueUrl;
    this.debug('sqs put queue dead url: ' + this.putQueueDeadUrl);

    // Now we'll find the dead letter put queue's ARN
    awsRes = await this.sqs.getQueueAttributes({
      QueueUrl: this.putQueueDeadUrl,
      AttributeNames: ['QueueArn'],
    }).promise();
    let putQueueDeadArn = awsRes.data.Attributes.QueueArn;
    this.debug('sqs put queue dead arn: ' + putQueueDeadArn);

    // First, we'll create the Queue
    this.debug('creating sqs put queue');
    awsRes = await this.sqs.createQueue({
      QueueName: this.putQueueName,
      Attributes: {
        RedrivePolicy: JSON.stringify({
          maxReceiveCount: 5,
          deadLetterTargetArn: putQueueDeadArn,
        }),
      },
    }).promise();
    this.debug('created sqs put queue');
    this.putQueueUrl = awsRes.data.QueueUrl;
    this.debug('sqs put queue url: ' + this.putQueueUrl);

    // Create consumer
    this.consumer = SQSConsumer.create({
      queueUrl: this.putQueueUrl,
      batchSize: this.sqsBatchSize,
      handleMessage: function(message, done) {
        if (message.Body) {
          let body;

          try {
            body = JSON.parse(message.Body);
          } catch (err) {
            this.debug(`error parsing ${message.Body}: ${err.stack || err}`);
            done(err);
          }

          this.debug(`received put request for ${body.url}`);

          let p = this.put.call(this, body.url);

          p = p.then(() => {
            this.debug(`completed put request for ${body.url}`);
            done();
          });

          p.catch(err => {
            this.debug(`error handling put request ${err.stack || err}`);
            done(err);
          });

        } else {
          this.debug('received empty sqs message, ignoring');
        }
      }.bind(this),
      sqs: this.sqs,
    });
  }

  startListeningToPutQueue() {
    this.debug('listneing to put queue ' + this.putQueueName);
    this.consumer.start();
  }

  stopListeningToPutQueue() {
    this.debug('no longer listneing to put queue ' + this.putQueueName);
    this.consumer.stop();
  }

  async put(rawUrl) {
    assert(rawUrl);
    this.debug(`putting ${rawUrl}`);

    let m = meter();
    m.on('error', err => {
      this.debug(`error from stream-meter ${err.stack || err}`);
    });

    this.debug(`creating read stream for ${rawUrl}`);
    let inputUrlInfo = await this.createUrlReadStream(rawUrl);
    this.debug(`created read stream for ${rawUrl}`);

    let inputStream = inputUrlInfo.stream;

    // We need the following pieces of information in the service-specific
    // implementations
    let contentType = inputUrlInfo.meta.headers[inputUrlInfo.meta.caseless.has('content-type')];
    contentType = contentType || 'application/octet-stream';
    let upstreamEtag = inputUrlInfo.meta.headers[inputUrlInfo.meta.caseless.has('etag')];
    upstreamEtag = upstreamEtag || '';
    let contentEncoding = inputUrlInfo.meta.headers[inputUrlInfo.meta.caseless.has('content-encoding')];
    let contentDisposition = inputUrlInfo.meta.headers[inputUrlInfo.meta.caseless.has('content-disposition')];

    let headers = {
      'Content-Type': contentType,
      'Content-Disposition': contentDisposition,
      'Content-Encoding': contentEncoding,
    };

    let storageMetadata = {
      'upstream-etag': upstreamEtag,
      url: rawUrl,
      stored: new Date().toISOString(),
      addresses: JSON.stringify(inputUrlInfo.addresses),
    };

    let startTime = new Date();
    try {
      await this.storageProvider.put(inputUrlInfo.url, inputStream, headers, storageMetadata);
    } catch (err) {
      this.debug(`error trying to put object: ${err.stack || err}`);
      await this.insertCacheEntry(rawUrl, 'error', this.cacheTTL, err.stack || err);
      return;
    }

    let duration = new Date() - startTime;

    // Here's a datapoint when we have metrics
    let dataPoint = {
      id: this.id,
      url: rawUrl,
      duration: duration,
      fileSize: m.bytes,
    };

    this.debug(`uploaded ${rawUrl} ${m.bytes} bytes in ${duration/1000} seconds`);

    await this.insertCacheEntry(rawUrl, 'present', this.cacheTTL);

  }

  async getUrlForRedirect(rawUrl) {
    let cacheEntry = await this.readCacheEntry(rawUrl);

    let worldAddress = this.storageProvider.worldAddress(rawUrl);

    if (!cacheEntry) {
      this.debug('cache entry not found for ' + rawUrl);
      let head = requestPromise.head({
        url: worldAddress,
        followRedirect: true,
        maxRedirects: this.redirectLimit,
      });

      if (head.statusCode >= 200 && head.statusCode < 300) {
        this.debug(`found ${rawUrl} in storageProvider, backfilling cache`);

        let expires = await this.storageProvider.expirationDate(head);

        let setTTL = expires - new Date();
        setTTL /= 1000;
        setTTL -= 30 * 60;

        this.debug(`backfilling cache for ${rawUrl}`);
        await this.insertCacheEntry(rawUrl, 'present', Math.floor(setTTL));
        this.debug(`backfilled cache for ${rawUrl}`);
        return {
          status: 'present',
          url: worldAddress,
        };
      } else {
        this.debug(`did not find ${rawUrl} in cache, inserting`);
        this.requestPut(rawUrl);
        return {
          status: 'pending',
          url: worldAddress,
        };
      }
    } else if (cacheEntry.status === 'present') {
      return {
        status: 'present',
        url: worldAddress,
      };
    } else if (cacheEntry.status === 'pending') {
      return {
        status: 'pending',
        url: worldAddress,
      };
    } else if (cacheEntry.status === 'error') {
      this.debug(`previous status was error: ${cacheEntry.stack}`);
      this.requestPut(rawUrl);
      return {
        status: 'pending',
        url: worldAddress,
      };
    } else {
      throw new Error('cacheEntry has invalid state ' + JSON.stringify(cacheEntry));
    }

  }

  async purge(rawUrl) {
    assert(rawUrl);
    this.debug(`removing ${rawUrl} from storageProvider`);
    await this.storageProvider.purge(rawUrl);
    this.debug(`removed ${rawUrl} from storageProvider`);
    this.debug(`removing cache entry for ${rawUrl}`);
    await this.redis.delAsync(this.cacheKey(rawUrl));
    this.debug(`removed cache entry for ${rawUrl}`);
  }

  async createUrlReadStream(rawUrl) {
    assert(rawUrl);
    let urlInfo = await followRedirects(rawUrl, this.allowedPatterns, this.redirectLimit, this.ensureSSL);

    let obj = request.get({
      uri: urlInfo.url,
      headers: {
        'Accept-Encoding': '*',
      },
    });

    obj.on('error', err => {
      this.debug(`error reading input url stream ${err.stack || err}`);
      throw err;
    });

    // We use a Passthrough to ensure that the return from the request library
    // is properly treated as a Stream and accessed only with the Stream API.
    // Without this I found that the AWS SDK would try to serialise the Request
    // object into JSON and upload that.  Yay software!
    let passthrough = new stream.PassThrough();

    return {
      stream: obj.pipe(passthrough),
      url: urlInfo.url,
      meta: urlInfo.meta,
      addresses: urlInfo.addresses,
    };
  }

  cacheKey(rawUrl) {
    return this.id + '_' + encodeURIComponent(rawUrl);
  }

  async insertCacheEntry(rawUrl, status, ttl, stack) {
    assert(rawUrl);
    assert(status);
    assert(ttl);
    assert(_.includes(CACHE_STATES, status));
    if (status === 'error') {
      assert(stack);
    }

    let cacheEntry = {
      url: rawUrl,
      status: status,
    };

    if (status === 'error') {
      cacheEntry.stack = stack;
    }

    let key = this.cacheKey(rawUrl);
    await this.redis.multi()
      .hmset(key, cacheEntry)
      .expire(key, ttl)
      .execAsync();
  }

  async readCacheEntry(rawUrl) {
    assert(rawUrl);
    let key = this.cacheKey(rawUrl);
    this.debug(`reading cache entry for ${rawUrl}`);
    let result = await this.redis.hgetallAsync(key);
    this.debug(`read cache entry for ${rawUrl}`);
    return result;
  }

  async requestPut(rawUrl) {
    assert(rawUrl);
    this.debug(`sending put request for ${rawUrl}`);
    await this.sqs.sendMessage({
      QueueUrl: this.putQueueUrl,
      MessageBody: JSON.stringify({
        url: rawUrl,
      }),
    }).promise();
    this.debug(`sent put request for ${rawUrl}`);
  }
}

module.exports = {
  CacheManager
};
