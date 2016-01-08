let assert = require('assert');
let crypto = require('crypto');
let url = require('url');
let debug = require('debug')(require('path').relative(process.cwd(), __filename));
let http = require('http');
let request = require('request').defaults({
  followRedirect: false, 
});
let fs = require('fs');
let stream = require('stream');
let meter = require('stream-meter');
let contentDisposition = require('content-disposition');
let SQSConsumer = require('sqs-consumer');

function delayer (time) {
  return new Promise(resolve => {
    setTimeout(resolve, time);
  });
}

/**
 * Obtain a header from a URL.  Do not follow redirects.  Returns the headers,
 * a caseless to look up header names without worrying about case, the HTTP
 * status code and the HTTP status message
 */
let requestHead = async (u) => {
  return new Promise((resolve, reject) => {
    request.head(u, {followRedirect: false}, (err, response) => {
      if (err) {
        debug(err.stack || err);
        return reject(err);
      }
      debug('got headers');
      resolve({
        headers: response.headers,
        caseless: response.caseless,
        statusCode: response.statusCode,
        statusMessage: response.statusMessage,
      });
    });
  });
}

/**
 * Follow redirects in a secure way, unless configured not to.  This function
 * will ensure that all redirects in a redirect chain are pointing to HTTPS
 * urls and not HTTP urls.  This is used to ensure that everything in the Cloud
 * Mirror storage was obtained through with an HTTP chain of custody.  If the 
 * config parameter 'allowInsecureRedirect' is a truthy value, HTTP urls and 
 * redirections from HTTPS to HTTP resources will be allowed.
 *
 * TODO:
 * - Ensure that the certificate validation being done here is using
 *   certificates that we're comfortable with.
 * - Decide whether the allowedPatterns should factor in here.  Arguably, if we
 *   start the chain with an allowed url, the redirects after that could be
 *   considered and implementation detail of the originating url's content
 *   owner.  The counter argument is that if we only allow some URLs, that we
 *   should ensure that nothing in the redirect chain is from a site other than
 *   those.  This can be checked for after this call by using the addresses
 *   property of the resolution object.
 */
let followRedirects = async (firstUrl, cfg = {}) => {
  // Number of redirects to follow
  let limit = cfg.limit || 10;
  let addresses = [];

  // What we're saying here is that all URLs passed through the redirector must
  // start with https: in order to avoid causing this function to throw
  let validateUrl = (u) => {
    if (!u.match(/^https:/)) {
      let s = 'Refusing to follow unsafe redirects: ';
      s += addresses.map(x => x.url).join(' --> ');
      s += u;

      let err = new Error(s);
      err.addresses = addresses;
      throw err;
    }
  }

  // If we aren't enforcing secure redirects, it's just easier to make the
  // validation function a no-op
  if (cfg.allowInsecureRedirect) {
    validateUrl = () => true;
  }

  // the u variable points to the URL in the current redirect chain
  let u = firstUrl;

  // the c variable is short for continue and is used to decide whether
  // to continue following redirects
  let c = true;

  for (let i = 0 ; c && i < limit ; i++) {
    validateUrl(u);
    let result = await requestHead(u);
    let sc = result.statusCode;

    // We store the chain of URLs that make up this redirection.  This could be
    // used if we wished to provide an audit trail in consuming systems
    addresses.push({
      code: sc,
      url: u,
      t: new Date(),
    });

    if (sc >= 200 && sc < 300) {
      // A 200 series redirect means that we're done.  We will not follow a 200
      // URL that has a Location: header because that's not part of the spec.
      // We can easily change this bit of code to make the choice of redirecting or
      // not based on the presence of the Location: header later if we choose.
      debug('Follwed all redirects: ' + addresses.map(x => x.url).join(' --> '));
      return {
        url: u,
        meta: result,
        addresses,
      };
    } else if (sc >= 300 && sc < 400 && sc !== 304 && sc !== 305) {
      // 304 and 305 are not redirects
      assert(result.caseless.has('location'));
      let newU = result.headers[result.caseless.has('location')];
      newU = url.resolve(u, newU);
      u = url.resolve(u, newU);
    } else {
      // Consider using an exponential backoff and retry here
      throw new Error('HTTP Error while redirecting');
    }
  }
  throw new Error(`Limit of ${limit} redirects reached: ${addresses.map(x => x.url).join(' --> ')}`);
};

/**
 * We use this to wrap the upload object returned by s3.upload so that we get a
 * promise interface.  Since this api method does not return the standard
 * AWS.Result class, it's not wrapped in Promises by the aws-sdk-promise
 * wrapper.  Maybe we should consider adding this wrapper to the
 * aws-sdk-promise class...
 */
let wrapSend = (upload) => {
  return new Promise((res, rej) => {
    // TODO: Make this configurable?
    let abortTimer = setTimeout(upload.abort.bind(upload), 1000 * 60 * 60);
    debug('initiating upload');
    upload.send((err, data) => {
      clearTimeout(abortTimer);
      if (err) {
        debug('upload error');
        debug(err.stack || err);
        rej(err);
      } else {
        debug('upload completed');
        res(data);
      }
    });
  });
};
//http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/index.html

/**
 * This class represents the operations that a backend must support to be used
 * by s3-distribute.  All `rawUrl`s passed in must be raw and not using any encoding
 * scheme.  An example of this would be a StorageBackend for the US-West-1 region of
 * S3.
 *
 * Options:
 *  - id: this is the identifier for a given backend.  It's used to uniquely
 *  identify this particular backend.  This should be unique to the
 *  billing/usage grouping.  For example, we could have two S3Backend instances
 *  for us-west-2, but because those two backends do work in the same region
 *  and have the same billing impact they share an id
 *  - allowedPatterns: each URL passed into the put() method to insert into our
 *  cache must match at least one of these regular expressions
 *  - urlTTL: the amount of time that a cache entry is put into memcached for
 *  - memcached: a reference to the memcached object we will use to cache our
 *  backend addresses
 *  - sqs: a reference to an sqs object which will be used to listen for
 *  incoming copy requests as well as to send out completion messages
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

  /**
   * Because constructors are not able to be asyncronous, we have an async
   * init method which will be used to do all async setup
   *
   * We should probably have a method in the API which we use to register
   * a given storage backend's copy request queue along with the metadata
   * for which region it is... that way we don't have to rely on our URLs
   * being consistently generated in the same format
   */
  async init() {
    let data = await this.sqs.createQueue({
      QueueName: this.copyRequestQueueName,
    }).promise();

    // It seems like SQSConsumer rebinds the functions
    // and I don't want to do let that = this;
    let that = this;

    this.copyRequestQueue = data.data.QueueUrl;
    this.consumer = SQSConsumer.create({
      queueUrl: this.copyRequestQueue,
      handleMessage: (message, done) => {
        if (message.Body) {
          let body;
          try {
            body = JSON.parse(message.Body);
          } catch (err) {
            done(err);
          }
          debug(`Received put request for ${that.id}: ${body.url}`);
          that.put.call(that, body.url).then(() => {
            debug(`Put request Completed for ${that.id}: ${body.url}`);
            done();
          }, err => {
            debug('Error calling put in handler: ' + err.stack || err); 
            done();
          });
        } else {
          debug('Received an empty message, huh?');
          done();
        }
      },
      sqs: this.sqs,
    });
  }

  /**
   * Listen to the Copy Request Queue
   */
  async startListeningToRequestQueue() {
    this.consumer.start();
  }

  async stopListeningToRequestQueue() {
    this.consumer.stop();
  }

  /**
   * High level API which inserts a resource specified by a given URL into the
   * cache.  In this base class, we take care of everything that is not tied to
   * the individual storage backend system requirements.  This means that
   * implementing a new backend means only understanding how to do the
   * low-level operations unique to that platform
   */
  async put(rawUrl) {
    /* NEED TO MAKE SURE ALLOWEDPATTERNS WORKS */
    /*let valid = false;
    for (let pattern of this.allowedPatterns) {
      if (pattern.match(rawUrl)) {
        valid = true;
      }
    }
    if (!valid) {
      let err = new Error('URL Does not match any allowed patterns');
      err.url = rawUrl;
      err.code = 'NotAllowedUrl';
      throw err;
    }*/

    debug(`Putting "${rawUrl}" into mirrored files`);
    let cacheName = encodeUrl(rawUrl);
    debug(`Encoded URL: ${cacheName}`);

    let backendAddress = this.backendAddress(rawUrl);

    await this.storeAddress(rawUrl, 'pending', backendAddress);

    // I should create a queue for the pending copy and send
    // a message to it when the copy completes

    // The meter is basically a passthrough stream which lets me count the
    // number of bytes that go through it.  This lets us collect metrics on the
    // copying operations better
    let m = meter();
    m.on('error', err => {
      debug('error from stream-meter: %s', err.stack||err);
    });

    // Get a stream for the input URL as well as some metadata
    let readInfo = await this.prepareUrlRead(rawUrl);

    // Set up the actual stream with the usage meter so that we can
    // pass that through to the 
    let readStream = readInfo.stream.pipe(m);
    debug(`Creating read stream for ${rawUrl}`);

    // Figure out the name
    debug(`Backend Address: ${JSON.stringify(backendAddress)}`);

    // We need the following pieces of information in the service-specific
    // implementations 
    let contentType = readInfo.meta.headers[readInfo.meta.caseless.has('content-type')];
    contentType = contentType || 'application/octet-stream';
    let upstreamEtag = readInfo.meta.headers[readInfo.meta.caseless.has('etag')];

    // We want metrics on how long it is taking us to transfer files.  This is
    // done here instead of in the backend because we want to ensure that all
    // overhead as well as transfer times are taken into account
    let startTime = new Date();
    try {
      await this._put(backendAddress,
                      readStream,
                      readInfo.url,
                      contentType,
                      readInfo.addresses,
                      upstreamEtag);
    } catch (err) {
      await this.storeAddress(rawUrl, 'error', {}, err.stack || err);
      return;
    }

    // When we start submitting things to Influx, this is the datapoint to use
    let duration = new Date() - startTime;
    let dataPoint = {
      id: this.id,
      inputUrl: rawUrl, 
      outputUrl: 'notused',
      duration: duration,
      fileSize: m.bytes,
    }

    debug(`Uploaded ${m.bytes} byte file in ${duration/1000}s`);

    // Now that the resource is in the cache, we want to make sure that we
    // reflect that in our memcached.  We do this by setting the status
    // property of the memcache value to be present.
    await this.storeAddress(rawUrl, 'present', backendAddress);

    return;
  }

  /**
   * Prepare the needed things for reading from a URL.
   * At present this means determining the fully redirected
   * HTTP Url after following only secure redirects as well 
   * as getting the actual headers for the final resulting
   * URL.
   */
  async prepareUrlRead(rawUrl) {
    let urlInfo = await followRedirects(rawUrl, {
      limit: 20,
      allowInsecureRedirect: true,
    });

    let obj = request.get(urlInfo.url);

    obj.on('error', err => {
      debug(err.stack || err);
      throw err;
    });

    // We use a Passthrough to ensure that the return from the request library
    // is properly treated as a Stream and accessed only with the Stream API.
    // Without this I found that the AWS Sdk would try to serialise the Request
    // object with JSON and upload that.  Yay software!
    let passthrough = new stream.PassThrough();

    return {
      stream: obj.pipe(passthrough),
      url: urlInfo.url,
      meta: urlInfo.meta,
      addresses: urlInfo.addresses,
    };
  }

  async storeAddress(rawUrl, status, backendAddress, stack) {
    assert(rawUrl);
    assert(status);
    assert(backendAddress);

    let memcachedEntry = {
      originalUrl: rawUrl,
      status: status,
      backendAddress: backendAddress,
    }

    if (status === 'error') {
      assert(stack);
      memcachedEntry.stack = stack;
    } else {
      assert(!stack);
    }
    let key = encodeURIComponent(rawUrl); 
    let jsonVersion = JSON.stringify(memcachedEntry);
    debug(`MEMCACHE SET: ${key}, ${jsonVersion}, ${this.urlTTL}`);
    await this.memcached.set(key, jsonVersion, this.urlTTL);
  }

  async readAddressFromCache(rawUrl) {
    assert(rawUrl);
    let key = encodeURIComponent(rawUrl);
    let jsonVersion = await this.memcached.get(key);
    debug(`MEMCACHE GET: ${key}, ${jsonVersion}`);
    return jsonVersion ? JSON.parse(jsonVersion) : undefined;
  }

  /**
   * Stub method for the backend specific operations to upload the resource
   * represented by 'inStream' to the address represented by 'name'.  The
   * original URL, contentType, redirects and upstreamEtag will be passed into
   * this method for use in the upload process.
   *
   * The backend implementation must return a promise that resolves to the
   * information that can be used to identify the copy of this resource that it
   * contains.
   */
  async _put(name, inStream, _url, contentType, redirects, upstreamEtag) {
    throw new Error('Putting not yet implemented');
  }

  /**
   */
  async requestPut(rawUrl) {
    let sqsParams = {
      QueueUrl: this.copyRequestQueue,
      MessageBody: JSON.stringify({
        url: rawUrl,
      }),
    };
    await this.sqs.sendMessage(sqsParams).promise();
  }

  /**
   */
  async getBackendUrl(rawUrl) {
    let cacheEntry = await this.readAddressFromCache(rawUrl);

    let backendUrl = this.backendAddressToUrl(this.backendAddress(rawUrl));

    if (!cacheEntry) {
      debug(`Cache entry not found for ${rawUrl}`);
      // FIRST CHECK IF THE THING EXISTS FOR REAL AND INSERT
      // IF IT DOES!
      let headers = await requestHead(backendUrl);
      debug(headers.statusCode);
      if (headers.statusCode >= 200 && headers.statusCode < 300) {
        debug(`Found ${rawUrl} in backend, caching values`);
        await this.storeAddress(rawUrl, 'present', this.backendAddress(rawUrl));
        return {
          status: 'present',
          url: backendUrl,
        }
      } else {
        debug(`Did not find ${rawUrl} in backend, inserting`);
        this.requestPut(rawUrl);
        return { 
          status: 'pending',
          url: backendUrl,
        };
      }
      
    }
    
    if (cacheEntry.status === 'error') {
      this.requestPut(rawUrl);
      return { 
        status: 'pending',
        url: backendUrl,
      };
    } else if (cacheEntry.status === 'pending') {
      return { 
        status: 'pending',
        url: backendUrl,
      };
    } else if (cacheEntry.status === 'present') {
      debug(`Cache entry found for ${rawUrl} found`);
      return {
        status: 'present',
        url: backendUrl,
      };
    }

    return returnValue;

  }


  /**
   * Remove all caches for a given URL
   */
  async expire(rawUrl) {
    let url = encodeUrl(rawUrl);
    let name = this.backendAddress(rawUrl);
    this._expire(name);
    // Here's where we'd delete from database and 
    await this.memcached.del(url);
  }

  async _expire(name) {
    throw new Error('Expiring not yet implemented');
  }

  // Create a name for the storage subsystem that we'll
  // use to address a resource.  This can be a one way mapping
  backendAddress(rawUrl) {
    assert(rawUrl.length <= 1024, 's3 key must be 1024 or fewer unicode characters');
    // Use this header to set the download name: 
    // http://www.w3.org/Protocols/rfc2616/rfc2616-sec19.html#sec19.5.1
    return encodeURIComponent(rawUrl);
  }

  // Create a real URL from a backend address
  backendAddressToUrl(backendAddress) {
    // We throw here because it's impossible to know how the implementing class
    // will map this backend address into a real url.  Simply decoding the URL
    // would mean that the cache isn't used and that's the only thing we know
    // how to do with this data 
    throw new Error('Unimplemented');
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
    // This will become a way to switch how we do S3 uploading
    this.uploadMethod = this._putUsingS3Upload;
  }

  async _expire(name) {
    await this.s3.deleteObject({
      Bucket: this.bucket,
      Key: name.key,
    }).promise();
  }

  async _putUsingS3Upload(inStream, name, url, contentType, redirects, upstreamEtag) {
    assert(inStream);
    assert(name);
    assert(url);
    assert(contentType);
    assert(redirects);
    let metadata = {
      url: url,
      stored: new Date().toISOString(),
    };

    // We want to show the upstream's ETag to make debugging easier
    if (upstreamEtag) {
      metadata['upstream-etag'] = upstreamEtag;
    }

    // We want to expose the set of redirects that occured when putting this
    // item into the cache.  We only want this when explicitly requested because
    // there's no reason to expose this much info by default
    if (process.env.SET_REDIRECTS_HEADER) {
      metadata.redirects = JSON.stringify(redirects);
    }

    // We need to URL decode the Key because the S3 library
    // is smart enough to URL encode itself, and we otherwise
    // end up with double encoding
    let request = {
      Bucket: this.bucket,
      Key: decodeURIComponent(name.key),
      Body: inStream,
      ContentType: contentType,
      ContentDisposition: contentDisposition(name.filename),
      ACL: 'public-read',
      Metadata: metadata,
    };
    let options = {
      partSize: 32 * 1024 * 1024,
      queueSize: 4,
    };
    debug('s3.upload starting');
    let upload = this.s3.upload(request, options);
    /*upload.on('httpUploadProgress', progress => {
      debug(`  * HTTP Upload Progress: ${progress.loaded}`);
    });*/
    let result = await wrapSend(upload);
    debug('s3.upload result: %j', result);
    return result
  }

  async _put(name, inStream, url, contentType, redirects, upstreamEtag) {
    assert(name, 'missing name for _put');
    assert(inStream, 'missing inStream for _put');
    assert(url);
    assert(contentType);
    assert(redirects);
    assert(inStream instanceof stream.Readable, 'inStream must be stream');
    return await this.uploadMethod(inStream, name, url, contentType, redirects, upstreamEtag);
  }

  // For S3, we want to have a bucket name as well when we address
  // the resource
  backendAddress(rawUrl) {
    let urlparts = url.parse(rawUrl);
    let filename = urlparts.pathname.split('/');
    filename = filename[filename.length - 1];
    return {
      bucket: this.bucket,
      key: super.backendAddress(rawUrl),
      region: this.region,
      filename: filename,
    };
  }

  backendAddressToUrl(backendAddress) {
    // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
    let r = backendAddress.region;
    let s3Domain;

    if (r === 'us-east-1') {
      s3Domain = 's3.amazonaws.com';
    } else {
      s3Domain = `s3-${r}.amazonaws.com`;
    }

    let vhost = backendAddress.bucket + '.' + s3Domain;
    return url.format({
      protocol: 'https:',
      host: vhost,
      pathname: backendAddress.key
    });

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
  return encodeURIComponent(url.format(url.parse(rawUrl)));
}

/**
 * Decode a rawUrl for storage in the cache.  Currently this is a simple
 * rawUrl Decoding but might turn into something more complicated in future
 */
function decodeUrl(rawUrl) {
  return decodeURIComponent(url.format(url.parse(rawUrl)));
}

module.exports = {
  StorageBackend,
  S3Backend,
  encodeUrl,
  decodeUrl,
}
