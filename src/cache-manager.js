let debug = require('debug')('cloud-mirror:cache-manager');
let urllib = require('url');
let http = require('http');
let requestPromise = require('request-promise').defaults({
  followRedirect: false,
  simple: false,
  resolveWithFullResponse: true,
});
let request = require('./request').request;
let fs = require('fs');
let debugModule = require('debug');
let validateUrl = require('./validate-url');
let _ = require('lodash');
let assert = require('assert');

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

    this.urlValidator = (u) => {
      return validateUrl({
        url: u,
        allowedPatterns: this.allowedPatterns,
        ensureSSL: this.ensureSSL,
      });
    };
  }

  async put(rawUrl) {
    assert(rawUrl);
    this.debug(`putting ${rawUrl}`);

    // Tell others that we're working on this url
    await this.insertCacheEntry(rawUrl, 'pending', this.cacheTTL);

    // Basically, any error here should do the same thing: pring the exception
    // in our logs then set the cache entry to status === 'error'
    try {
      this.debug(`creating read stream for ${rawUrl}`);
      let inputUrlInfo = await this.createUrlReadStream(rawUrl);
      this.debug(`created read stream for ${rawUrl}`);

      let bytes = 0;

      let inputStream = inputUrlInfo.stream;

      inputStream.on('data', chunk => {
        bytes += chunk.length;
      });

      inputStream.on('error', async err => {
        console.error(err.stack || err);
        await this.storageProvider.purge(rawUrl);
        await this.insertCacheEntry(rawUrl, 'error', this.cacheTTL, err.stack || err);
      });

      let headers = {};

      // We need the following pieces of information in the service-specific
      // implementations
      let contentType = inputUrlInfo.headers['content-type'];
      if (contentType) {
        headers['Content-Type'] = contentType;
      }

      let contentEncoding = inputUrlInfo.headers['content-encoding'];
      if (contentEncoding) {
        headers['Content-Encoding'] = contentEncoding;
      }

      let contentDisposition = inputUrlInfo.headers['content-disposition'];
      if (contentDisposition) {
        headers['Content-Disposition'] = contentDisposition; 
      }

      let contentMD5 = inputUrlInfo.headers['content-md5'];
      if (contentMD5) {
        headers['Content-MD5'] = contentMD5;
      }

      let contentLength = inputUrlInfo.headers['content-length'];
      if (contentLength) {
        headers['Content-Length'] = contentLength;
      }

      let storageMetadata = {
        'cloud-mirror-upstream-etag': inputUrlInfo.headers['etag'] || '<unknown>',
        'cloud-mirror-upstream-content-length': contentLength || '<unknown>',
        'cloud-mirror-upstream-url': rawUrl,
        'cloud-mirror-stored': new Date().toISOString(),
        'cloud-mirror-addresses': JSON.stringify(inputUrlInfo.addresses),
      };

      let start = process.hrtime();

      inputStream.on('aborted', () => {
        inputStream.emit('error', new Error('Request aborted'));
      });
      inputStream.on('timeout', () => {
        inputStream.abort();
      });
      inputStream.setTimeout(1000 * 60 * 60);

      await this.storageProvider.put(rawUrl, inputStream, headers, storageMetadata);

      let d = process.hrtime(start);
      let duration = d[0] * 1000 + d[1] / 1000000;

      this.monitor.measure('copy-duration-ms', duration);
      this.monitor.measure('copy-size-bytes', bytes);
      let speed = bytes / duration / 1.024;
      this.monitor.measure('copy-speed-kbps', speed);

      this.debug(`uploaded ${rawUrl} ${bytes} bytes in ${duration/1000} seconds`);

      await this.insertCacheEntry(rawUrl, 'present', this.cacheTTL);

    } catch (err) {
      this.debug(`error putting ${rawUrl}: ${err.stack || err}`);
      await this.insertCacheEntry(rawUrl, 'error', this.cacheTTL, err.stack || err);
    }
  }

  async getUrlForRedirect(rawUrl) {
    let cacheEntry = await this.readCacheEntry(rawUrl);

    let worldAddress = this.storageProvider.worldAddress(rawUrl);

    let outcome = {
      url: worldAddress,
    };

    if (!cacheEntry) {
      this.debug('cache entry not found for %s (%s)', rawUrl, worldAddress);

      let cacheResource;

      // Since there's a good chance that we'll 404 here, which does throw
      // an error, we should catch it
      try {
        cacheResource = await this.urlValidator(worldAddress);
      } catch (err) {
        outcome.status = 'absent';
        return outcome;
      }

      if (cacheResource.statusCode >= 200 && cacheResource.statusCode < 300) {
        this.debug('found %s (%s) in storageProvider, backfilling cache', rawUrl, worldAddress);

        let expires = await this.storageProvider.expirationDate(cacheResource.headers);

        let setTTL = expires - new Date();
        setTTL /= 1000;
        setTTL -= 30 * 60;

        await this.insertCacheEntry(rawUrl, 'present', Math.floor(setTTL));
        this.debug(`backfilled cache for ${rawUrl}`);
        outcome.status = 'present';
        this.monitor.count('backfill', 1);
      } else {
        outcome.status = 'absent';
      }
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
    let upstreamResource = await this.urlValidator(rawUrl);

    let response = await request(upstreamResource.url, {
      headers: {
        'Accept-Encoding': '*',
      },
      allowUnsafeUrls: !this.ensureSSL,
    });

    return {
      stream: response,
      url: upstreamResource.url,
      headers: response.headers,
      addresses: upstreamResource.addresses,
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
    try {
      await this.redis.setAsync(key, JSON.stringify(cacheEntry), 'EX', ttl);
    } catch (err) {
      this.monitor.reportError(err);
      this.monitor.count('redis.cache-insert-failure', 1);
    }
  }

  async readCacheEntry(rawUrl) {
    assert(rawUrl);
    let key = this.cacheKey(rawUrl);
    let result = undefined;
    try {
      result = JSON.parse(await this.redis.getAsync(key));
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
    return result;
  }

  async requestPut(rawUrl) {
    assert(rawUrl);
    this.debug(`sending put request for ${rawUrl}`);
    await this.insertCacheEntry(rawUrl, 'pending', this.cacheTTL);
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
