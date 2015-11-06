let assert = require('assert');
let crypto = require('crypto');
let url = require('url');
let debug = require('debug')(require('path').relative(process.cwd(), __filename));
let http = require('http');
let request = require('request');
let fs = require('fs');

// NOTE: rawUrl == raw url as passed into this service
//       url == encoded url as used by caching
//       localUrl == local is defined as the storage cloud that we're trying to serve from

/**
 * This class represents the operations that a backend must support to be used
 * by s3-distribute.  All `rawUrl`s passed in must be raw and not using any encoding
 * scheme.  An example of this would be a StorageBackend for the US-West-1 region of
 * S3.
 */
class StorageBackend {
  constructor(config) {
    this.config = config;

    // A nice identifier for error messages
    assert(this.config.id, 'Missing storage backend ID');
    this.id = this.config.id;
    this.copyRequestQueueName = this.id + '_copy_requests';

    // Allowed Patterns is a list of regular expressions which will operate on decoded
    // rawUrls to determine if they are allowed in this system
    assert(this.config.allowedPatterns, 'Must specify allowed patterns URLs');
    this.allowedPatterns = this.config.allowedPatterns;

    // This is the number of seconds that we should keep the url in cache
    assert(this.config.urlTTL, 'Must specify how long to keep urls in cache');
    this.urlTTL = this.config.urlTTL;
    
    // A configured Memcached object which will be used to store the cache locations
    assert(this.config.memcached, 'Must provide a memcached object');
    this.memcached = this.config.memcached;

    // SQS Instance to use for communications
    assert(this.config.sqs, 'Must provide an SQS object');
    this.sqs = this.config.sqs;

    return this;
  }

  async init() {
    this.copyRequestsQueue = await this.sqs.createQueue({
      QueueName: this.copyRequestQueueName,
    });
    debug(`Copy Request Queue Name: ${this.copyRequestQueueName}`);
  }

  // Insert a rawUrl into the storage backend
  async put(rawUrl) {
    debug(`StorageBackend.put("${rawUrl}")`);
    let url = encodeUrl(rawUrl);
    debug(`Encoded URL: ${url}`);
    let val = {
      rawUrl: rawUrl,
      status: 'pending',
      backendName: this.storageSubsystemName(rawUrl), 
    };
    await this.memcached.set(url, JSON.stringify(val), this.urlTTL);
    debug(`Set memcached key ${url} to ${JSON.stringify(val)}`);
    // I should create a queue for the pending copy and send
    // a message to it when the copy completes
    let stream = this.readUrlStream(rawUrl);
    debug(`Creating read stream for ${rawUrl}`);
    let name = this.storageSubsystemName(rawUrl);
    let localUrl = await this._put(name, stream);
    val.status = 'present';
    // Here's where we'd store in the persistent data store
    await this.memcached.set(url, JSON.stringify(val), this.urlTTL);
    debug(`Set memcached key ${url} to ${JSON.stringify(val)}`);
    return localUrl;
  }

  readUrlStream(rawUrl) {
    //return fs.createReadStream('todo.txt');
    request(rawUrl);
  }

  // Implementors must take a raw URL, store it and return the address
  // to this resource
  async _put(name, inStream) {
    throw new Error('Putting not yet implemented');
  }

  async getUrl(rawUrl, doCopy) {
    let url = encodeUrl(rawUrl);
    let loadedUrl = JSON.parse(await this.memcached.get(url));
    let localUrl;

    if (!loadedUrl) {
      // Here's where we'd load from the persistent data store
      // and overwrite loadedUrl
    }

    if (!loadedUrl && doCopy) {
      return await this.put(rawUrl);
    } else if (!loadedUrl) {
      throw new Error('File not found in storage');
    } else {
      if (loadedUrl.status === 'present') {
        return loadedUrl.rawUrl;
      } else if (loadedUrl.status === 'pending') {
        // What I should do here is subscribe to an SQS queue that's addressed
        // by the encoded url
        throw new Error('copy is pending from something else');
      } else {
        throw new Error('Unexpected resource status');
      }
    }
  }

  async expire(rawUrl) {
    let url = encodeUrl(rawUrl);
    let name = this.storageSubsystemName(rawUrl);
    this._expire(name);
    // Here's where we'd delete from database and 
    await this.memcached.del(url);
  }

  async _expire(name) {
    throw new Error('Expiring not yet implemented');
  }

  // Create a name for the storage subsystem that we'll
  // use to address a resource.  This can be a one way mapping
  storageSubsystemName(rawUrl) {
    return crypto.createHash('sha256').update(rawUrl).digest('hex');
  }

}

class S3Backend extends StorageBackend {
  constructor(config) {
    assert(config.region, 'must specify aws s3 region');
    let superConfig = {
      urlTTL: config.urlTTL,
      sqs: config.sqs,
      memcached: config.memcached,
      allowedPatterns: config.allowedPatterns,
      id: 'S3_' + config.region,
    };
    super(superConfig);
    this.region = config.region;
    this.s3 = config.s3;
    this.bucket = config.bucket;
  }

  async _expire(name) {
    await this.s3.deleteObject({
      Bucket: this.bucket,
      Key: name.key,
    }).promise();
  }

  async _put(name, inStream) {
    let request = {
      Bucket: this.bucket,
      Key: name.key,
      Body: inStream,
      // Expires: ???
      // I wonder if we should also be setting ContentType:
    };
    debug('s3.putObject request: %j' , request);
    let result = await this.s3.putObject(request).promise();
    console.dir(result);
  }

  // For S3, we want to have a bucket name as well when we address
  // the resource
  storageSubsystemName(rawUrl) {
    let urlparts = url.parse(rawUrl);
    return {
      bucket: this.bucket,
      key: super.storageSubsystemName(rawUrl),
      region: this.region,
    };
  }
}

function validateBucketName(name, sslVhost = false) {
  // http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

  // Bucket names must be at least 3 and no more than 63 characters long.
  if (name.length < 3 || name.length > 63) {
    return false;
  }

  // Bucket names must be a series of one or more labels. Adjacent labels are
  // separated by a single period (.). Bucket names can contain lowercase
  // letters, numbers, and hyphens. Each label must start and end with a
  // lowercase letter or a number. 
  if (/\.\./.exec(name) || /^[^a-z0-9]/.exec(name) || /[^a-z0-9]$/.exec(name)) {
    return false;
  };
  if (! /^[a-z0-9-\.]*$/.exec(name)) {
    return false;
  }

  //Bucket names must not be formatted as an IP address (e.g., 192.168.5.4)
  // https://www.safaribooksonline.com/library/view/regular-expressions-cookbook/9780596802837/ch07s16.html
  if (/^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/.exec(name)) {
    return false;
  }

  // When using virtual hosted–style buckets with SSL, the SSL wild card
  // certificate only matches buckets that do not contain periods. To work
  // around this, use HTTP or write your own certificate verification logic.
  if (sslVhost) {
    if (/\./.exec(name)) {
      return false
    }
  }

  return true;
}


/**
 * Encode a rawUrl for storage in the cache.  Currently this is a simple
 * rawUrl Encoding but might turn into something more complicated in future
 */
function encodeUrl(rawUrl) {
  return encodeURIComponent(rawUrl);
}

/**
 * Decode a rawUrl for storage in the cache.  Currently this is a simple
 * rawUrl Decoding but might turn into something more complicated in future
 */
function decodeUrl(rawUrl) {
  return decodeURIComponent(rawUrl);
}

module.exports = {
  StorageBackend,
  S3Backend,
  encodeUrl,
  decodeUrl,
}
