let StorageProvider = require('./storage-provider.js').StorageProvider;
let url = require('url');
let assert = require('assert');
let debug = require('debug')('cloud-mirror:s3-storage-provider');
let stream = require('stream');
let cookie = require('cookie');
let _ = require('lodash');

/**
 * We use this to wrap the upload object returned by s3.upload so that we get a
 * promise interface.  Since this api method does not return the standard
 * AWS.Result class, it's not wrapped in Promises by the aws-sdk promise
 * wrapper.  Maybe we should consider adding this wrapper to the
 * aws-sdk class...
 */
let wrapSend = (bucket, key, upload) => {
  return new Promise((res, rej) => {
    // TODO: Make this configurable?
    let abortTimer = setTimeout(() => {
      upload.abort();
    }, 1000 * 60 * 60 * 2);

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
 * Mapping of HTTP Headers to S3 request properties since they don't use the
 * same string.
 */
const HTTPHeaderToS3Prop = {
  'Content-Type': 'ContentType',
  'Content-Disposition': 'ContentDisposition',
  'Content-MD5': 'ContentMD5',
  'Content-Encoding': 'ContentEncoding',
  'Content-Length': 'ContentLength',
};

/**
 * HTTP Headers which *must* be specified during an upload
 */
const MandatoryHTTPHeaders = ['Content-Type'];

/**
 * HTTP Headers which *must not* be specified during an upload
 */
const DisallowedHTTPHeaders = ['Cache-Control', 'Expires'];

/**
 * Implementation of the StorageProvider for S3
 */
class S3StorageProvider extends StorageProvider {

  constructor(config) {
    super(config);
    assert(config.bucket, 'must specify an s3 bucket');
    assert(config.partSize, 'must specify an upload part size');
    assert(config.queueSize, 'must specify an upload queue size');
    assert(config.s3, 'must provide an S3 object');
    assert(config.acl, 'must provide acl value for bucket');
    assert(config.lifespan, 'must provide lifespan value for bucket');
    this.region = config.region;
    this.bucket = config.bucket;
    this.partSize = config.partSize;
    this.queueSize = config.queueSize;
    this.s3 = config.s3;
    this.acl = config.acl;
    this.lifespan = config.lifespan;
  }

  /**
   * Ensure that our bucket exists
   */
  async init() {
    this.debug(`creating ${this.bucket}`);
    await createS3Bucket(this.s3, this.bucket, this.region, this.acl, this.lifespan);
    this.debug(`creating ${this.bucket}`);
  }

  /**
   * StorageProvider.put() implementation for S3
   */
  async put(rawUrl, inputStream, headers, storageMetadata) {
    assert(rawUrl, 'must provide raw input url');
    assert(inputStream, 'must provide an input stream');
    assert(headers, 'must provide HTTP headers');
    assert(storageMetadata, 'must provide storage provider metadata');

    // We decode the key because the S3 library 'helpfully'
    // URL encodes this value
    let request = {
      Bucket: this.bucket,
      Key: rawUrl,
      Body: inputStream,
      ACL: 'public-read',
      Metadata: storageMetadata,
    };

    _.forEach(HTTPHeaderToS3Prop, (s3Prop, httpHeader) => {
      if (_.includes(DisallowedHTTPHeaders, httpHeader)) {
        throw new Error(`The HTTP header ${httpHeader} is not allowed`);
      } else if (_.includes(MandatoryHTTPHeaders, httpHeader)) {
        assert(headers[httpHeader], `HTTP Header ${httpHeader} must be specified`);
      }
      if (headers[httpHeader]) {
        request[s3Prop] = headers[httpHeader];
      }
    });

    let options = {
      partSize: this.partSize,
      queueSize: this.queueSize,
    };

    let upload = this.s3.upload(request, options);

    this.debug('starting S3 upload');
    let result;
    try {
      result = await wrapSend(this.bucket, rawUrl, upload);
    } catch (err) {
      
    }
    this.debug('completed S3 upload');
    return result;
  }

  /**
   * StorageProvider.purge() implementation for S3
   */
  async purge(rawUrl) {
    this.debug(`purging ${rawUrl} from ${this.bucket}`);
    await this.s3.deleteObject({
      Bucket: this.bucket,
      Key: rawUrl,
    }).promise();
    this.debug(`purged ${rawUrl} from ${this.bucket}`);
  }

  /**
   * Retreive the expiration time of object in S3
   *
   * This is stored in the format:
   * expiry-date="Fri, 15 Jan 2016 00:00:00 GMT", rule-id="eu-central-1-1-day"
   */
  async expirationDate(response) {
    let header = response.headers['x-amz-expiration'];
    if (!header) {
      throw new Error(JSON.stringify(response.headers, null, 2));
    }
    // This header is sent in such a silly format.  Using cookie format or
    // sending the value without packing it in with a second value (rule-id)
    // would be way nicer.
    // The requirements for this to stop being valid are so obscure that I
    // would wager that the whole format of the header changes and this entire
    // function would need to be rewritten as oppsed to the string replacement
    // You'd need to have inside the expiry-date value or key an escaped quote
    // that's followed by a comma...
    header = cookie.parse(header.replace('",', '";'));
    return new Date(header['expiry-date']);
  }

  /**
   * Create an S3 URL for an object stored in this S3 storage provider
   */
  worldAddress(rawUrl) {
    assert(rawUrl);
    let s3Domain;
    if (this.region === 'us-east-1') {
      s3Domain = 's3.amazonaws.com';
    } else {
      s3Domain = `s3-${this.region}.amazonaws.com`;
    }

    let host = this.bucket + '.' + s3Domain;

    return url.format({
      protocol: 'https:',
      host: host,
      pathname: encodeURIComponent(rawUrl),
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
      return false;
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
    };
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
          AbortIncompleteMultipartUpload: {
            DaysAfterInitiation: 1,
          },
          Expiration: {
            Days: lifecycleDays,
          },
        },
      ],
    },
  };

  debug(`Setting S3 lifecycle configuration for ${name} in ${region}`);
  await s3.putBucketLifecycleConfiguration(params).promise();
  debug(`Set S3 lifecycle configuration for ${name} in ${region}`);
}

module.exports = {
  S3StorageProvider,
  createS3Bucket,
};
