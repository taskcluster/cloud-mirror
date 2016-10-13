let debug = require('debug')('cloud-mirror:cache-manager');
let urllib = require('url');
let http = require('http');
let requestPromise = require('request-promise').defaults({
  followRedirect: false,
  simple: false,
  resolveWithFullResponse: true,
});
let request = require('request').defaults({
  followRedirect: false,
});
let fs = require('fs');
let stream = require('stream');
let debugModule = require('debug');
let validateUrl = require('./validate-url');
let _ = require('lodash');
let assert = require('assert');
let Mutex = require('shared-mutex').Mutex;

const CACHE_STATES = ['present', 'pending', 'error'];

class CacheManager {
  constructor(config) {
    for (let x of [
      'allowedPatterns', // Regular expressions to validate input urls
      'cacheTTL', // Number of seconds to keep URL in the cache
      'redis', // Redis object where we should cache metadata
      'ensureSSL', // true if we should force only HTTP in redirect links
      'storageProvider', // StorageProvider instance to manage
      'monitor', // taskcluster-lib-monitor instance
      'queueSender', // sqsSimple QueueSender
    ]) {
      assert(typeof config[x] !== 'undefined', `CacheManager requires ${x} configuration value`);
      this[x] = config[x];
    }

    // Maximum number of redirects to follow
    this.redirectLimit = config.redirectLimit || 30;

    this.queueSender = config.queueSender;

    // We'll use the same ID here as we have set in the storage provider
    this.id = this.storageProvider.id;
    this.debug = debugModule(`cloud-mirror:${this.constructor.name}:${this.id}`);
    
    this.monitor = config.monitor.prefix(this.id);
  }

  async put(rawUrl) {
    assert(rawUrl);
    this.debug(`putting ${rawUrl}`);

    let mutex = new Mutex(this.redis, this.lockKey(rawUrl));
    try {
      mutex.lock();
      this.monitor.count('cloud-mirror.concurrent-copy.no-issues', 1);
    } catch (err) {
      debug('WE ARE ATTEMPTING TWO CONCURRENT COPIES FOR SAME RESOURCE, IGNORING');
      this.monitor.count('cloud-mirror.concurrent-copy.already-locked', 1);
      throw err;
    }

    // Tell others that we're working on this url
    await this.insertCacheEntry(rawUrl, 'pending', this.cacheTTL);

    // Basically, any error here should do the same thing: pring the exception
    // in our logs then set the cache entry to status === 'error'
    try {
      let bytes = 0;

      this.debug(`creating read stream for ${rawUrl}`);
      let inputUrlInfo = await this.createUrlReadStream(rawUrl);
      this.debug(`created read stream for ${rawUrl}`);

      let inputStream = inputUrlInfo.stream;
      // We use a Passthrough to ensure that the return from the request library
      // is properly treated as a Stream and accessed only with the Stream API.
      // Without this I found that the AWS SDK would try to serialise the Request
      // object into JSON and upload that.  Yay software!

      inputStream.on('data', chunk => {
        bytes += chunk.length;
      });

      inputStream = inputStream.pipe(new stream.PassThrough());

      // We need the following pieces of information in the service-specific
      // implementations
      let contentType = inputUrlInfo.meta.headers['content-type'];
      contentType = contentType || 'application/octet-stream';
      let upstreamEtag = inputUrlInfo.meta.headers['etag'];
      upstreamEtag = upstreamEtag || '';
      let contentEncoding = inputUrlInfo.meta.headers['content-encoding'];
      let contentDisposition = inputUrlInfo.meta.headers['content-disposition'];
      let contentMD5 = inputUrlInfo.meta.headers['content-md5'];
      let contentLength = parseInt(inputUrlInfo.meta.headers['content-length'], 10);

      let headers = {
        'Content-Type': contentType,
        'Content-Disposition': contentDisposition,
        'Content-Encoding': contentEncoding,
        'Content-MD5': contentMD5,
        'Content-Length': contentLength,
      };

      let storageMetadata = {
        'upstream-etag': upstreamEtag,
        url: rawUrl,
        stored: new Date().toISOString(),
        addresses: JSON.stringify(inputUrlInfo.addresses),
      };

      if (inputUrlInfo.meta.headers['content-length']) {
        storageMetadata['upstream-content-length'] = inputUrlInfo.meta.headers['content-length'];
      }

      let start = process.hrtime();

      await this.storageProvider.put(rawUrl, inputStream, headers, storageMetadata);

      let d = process.hrtime(start);
      let duration = d[0] * 1000 + d[1] / 1000000;

      this.monitor.measure('copy-duration-ms', duration);
      this.monitor.measure('copy-size-bytes', bytes);
      let speed = bytes / duration / 1.024;
      this.monitor.measure('copy-speed-kbps', speed);

      this.debug(`uploaded ${rawUrl} ${bytes} bytes in ${duration/1000} seconds`);

      if (contentLength && contentLength !== bytes) {
        let errmsg = `content length of input ${contentLength} is different to amount of bytes uploaded ${bytes}`;
        this.debug(errmsg);
        let err = new Error(errmsg);
        err.upstreamLength = contentLength;
        err.bytesUploaded = bytes;
        // TODO: figure out why we're uploading more bytes than content length
        // is and re-enable this as well as moving the below insertCacheEntry
        // with 'present' back into the else of this if
        //await this.insertCacheEntry(rawUrl, 'error', this.cacheTTL, err.stack);
        //await this.storageProvider.purge(rawUrl);
        this.debug(`original content-length: ${contentLength} differs from size of upload ${bytes}`);
      } // else {
      this.debug('finished upload');
      await this.insertCacheEntry(rawUrl, 'present', this.cacheTTL);
      //}
    } catch (err) {
      this.debug(`error putting ${rawUrl}: ${err.stack || err}`);
      await this.storageProvider.purge(rawUrl);
      await this.insertCacheEntry(rawUrl, 'error', this.cacheTTL, err.stack || err);
    } finally {
      mutex.unlock();
    }
  }

  async getUrlForRedirect(rawUrl) {
    let cacheEntry = await this.readCacheEntry(rawUrl);

    let worldAddress = this.storageProvider.worldAddress(rawUrl);

    let outcome = {
      url: worldAddress,
    };

    if (!cacheEntry) {
      let mutex = new Mutex(this.redis, this.lockKey(rawUrl));
      try {
        mutex.lock();
        this.monitor.count('cloud-mirror.concurrent-copy.no-issues', 1);
      } catch (err) {
        this.debug('WE ARE ATTEMPTING TO BACKFILL A PENDING COPY');
        this.monitor.count('cloud-mirror.concurrent-copy.already-locked', 1);
        throw err;
      }
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
        outcome.status = 'present';

        this.monitor.count('backfill', 1);
      } else {
        outcome.status = 'absent';
      }
      mutex.unlock();
    } else if (cacheEntry.status === 'present') {
      outcome.status = 'present';
    } else if (cacheEntry.status === 'pending') {
      outcome.status = 'pending';
    } else if (cacheEntry.status === 'error') {
      outcome.status = 'error';
    } else {
      throw new Error('cacheEntry has invalid state ' + JSON.stringify(cacheEntry));
    }
    
    return outcome;
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
    let urlInfo = await validateUrl(rawUrl, this.allowedPatterns, this.redirectLimit, this.ensureSSL, this.monitor);

    if (!urlInfo) {
      throw new Error('URL is invalid: ' + rawUrl);
    }

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

    return {
      stream: obj,
      url: urlInfo.url,
      meta: urlInfo.meta,
      addresses: urlInfo.addresses,
    };
  }

  lockKey(rawUrl) {
    return 'LOCK-' + this.cacheKey(rawUrl);
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
    try {
      await this.redis.multi()
        .hmset(key, cacheEntry)
        .expire(key, ttl)
        .execAsync();
    } catch (err) {
      this.monitor.reportError(err);
      this.monitor.count('redis.cache-insert-failure', 1);
    }
  }

  async readCacheEntry(rawUrl) {
    assert(rawUrl);
    let key = this.cacheKey(rawUrl);
    this.debug(`reading cache entry for ${rawUrl}`);
    let result = undefined;
    try {
      result = await this.redis.hgetallAsync(key);
    } catch (err) {
      this.monitor.reportError(err);
      this.monitor.count('redis.cache-read-failure', 1);
    }
    if (result) {
      assert(_.includes(CACHE_STATES, result.status));
      this.monitor.count('redis.cache-hit', 1);
    } else {
      this.monitor.count('redis.cache-miss', 1);
    }
    this.debug(`read cache entry for ${rawUrl}`);
    return result;
  }

  async deleteCacheEntry(rawUrl) {
    let key = this.cacheKey(rawUrl);
    try {
      await this.redis.delAsync(key);
    } catch (err) {
      this.monitor.reportError(err);
      this.monitor.count('redis.cache-delete-failure', 1);
    }
  }

  async requestPut(rawUrl) {
    assert(rawUrl);
    this.debug(`sending put request for ${rawUrl}`);
    // ID is the identifier for a storage pool.  This is a combination of the
    // service and the subdivison of that service
    await this.queueSender.insert({
      id: this.id,
      url: rawUrl,
      action: 'put',
    });
    this.debug(`sent put request for ${rawUrl}`);
  }
}

module.exports = {
  CacheManager,
};
