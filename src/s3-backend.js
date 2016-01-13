let storageBackend = require('./storage-backend.js');
let encodeURL = storageBackend.encodeURL;
let decodeURL = storageBackend.decodeURL;
let StorageBackend = storageBackend.StorageBackend;
let contentDisposition = require('content-disposition');
let url = require('url');
let assert = require('assert');
let debug = require('debug')('cloud-mirror:s3backend');
let stream = require('stream');

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

/**
 * Implementation of the StorageBackend class for S3.  This class will put all
 * cached files into the same bucket.
 */
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
    // Plugable upload methods.  Function will be bound to this object
    if (config.uploadMethod) {
      assert(typeof this[config.uploadMethod] === 'function');
      this.uploadMethod = this[config.uploadMethod].bind(this);
    } else {
      this.uploadMethod = this._putUsingS3Upload;
    }
  }

  /**
   * Delete from S3
   */
  async _expire(rawUrl) {
    await this.s3.deleteObject({
      Bucket: this.bucket,
      Key: encodeURL(rawUrl),
    }).promise();
  }

  /**
   * Upload a file to S3 using the aws-sdk upload method.  This method does multi-part
   * uploading and automatic cleanup of failed attemptes.
   */
  async _putUsingS3Upload(inStream, name, rawUrl, contentType, redirects, upstreamEtag) {
    assert(inStream);
    assert(name);
    assert(rawUrl);
    assert(contentType);
    assert(redirects);
    let metadata = {
      url: rawUrl,
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
      Key: decodeURL(name.key),
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
    debug(`${this.id} Starting S3 upload`);
    let upload = this.s3.upload(request, options);
    /*upload.on('httpUploadProgress', progress => {
      debug(`  * HTTP Upload Progress: ${progress.loaded}`);
    });*/
    let result = await wrapSend(upload);
    debug(`${this.id} Finished S3 upload`);
    return result
  }

  /**
   * Implementation of the base class
   */
  async _put(name, inStream, rawUrl, contentType, redirects, upstreamEtag) {
    assert(name, 'missing name for _put');
    assert(inStream, 'missing inStream for _put');
    assert(rawUrl);
    assert(contentType);
    assert(redirects);
    assert(inStream instanceof stream.Readable, 'inStream must be stream');
    return await this.uploadMethod(inStream, name, rawUrl, contentType, redirects, upstreamEtag);
  }

  /**
   * S3 Has some more useful information that we want to use to identify a
   * resource
   */
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

  /**
   * Given an S3 backendAddress, resolve a valid S3 URL.  Note that S3 does
   * funny things regarding the domain name of the URL.  For legacy reasons
   * the us-east-1 region does not have its region in the domain.  We could
   * use different hostnames for some EU and maybe southeast-Asia regions
   * but let's stick to the pattern
   */
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

// Validate an S3 bucket
function validateS3BucketName(name, sslVhost = false) {
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
 * Create an S3 Bucket in a specified region with the given name and ACL.  All
 * objects will expire after 'lifecycleDays' days have elapsed.
 */
async function createS3Bucket(s3, name, region, acl, lifecycleDays = 1) {
  assert(s3);
  assert(name);
  assert(region);
  assert(acl);
  assert(typeof lifecycleDays === 'number');
  if (!validateS3BucketName(name, true)) {
    throw new Error(`Bucket ${name} is not valid`);
  }
  let params = {
    Bucket: name,
    ACL: acl,
  };
  if (region !== 'us-east-1') {
    params.CreateBucketConfiguration = {
      LocationConstraint: region,
    }
  }

  try {
    debug(`Creating S3 Bucket ${name} in ${region}`);
    await s3.createBucket(params).promise();
    debug(`Created S3 Bucket ${name} in ${region}`);
  } catch (err) {
    switch (err.code) {
      case 'BucketAlreadyExists':
      case 'BucketAlreadyOwnedByYou':
        break;
      default:
        throw err;
    }
  }

  params = {
    Bucket: name,
    LifecycleConfiguration: {
      Rules: [
        {
          ID: region + '-' + lifecycleDays + '-day',
          Prefix: '',
          Status: 'Enabled',
          Expiration: {
            Days: lifecycleDays,
          },
        }
      ]
    }
  };

  debug(`Setting S3 lifecycle configuration for ${name} in ${region}`);
  await s3.putBucketLifecycleConfiguration(params).promise();
  debug(`Set S3 lifecycle configuration for ${name} in ${region}`);

  
}

module.exports = {
  S3Backend,
  createS3Bucket,
}

