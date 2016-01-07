"use strict";
let debug = require('debug')(require('path').relative(process.cwd(), __filename));
let aws = require('aws-sdk');
let sqs = new aws.SQS({apiVersion: '2012-11-05', region: 'us-west-2'});
let awsPromise = require('aws-sdk-promise');
let Memcached = require('memcache-promise');
let storageBackend = require('./storage-backend');
let memcached = new Memcached('localhost:11211');
//require('source-map-support/register')

let setupBackends = async function (config) {
  let awsBackends = [];
  let awsRegions = config.awsRegions;

  for (let region of awsRegions) {
    debug('Creating S3 Backend for ' + region);
    let s3 = new awsPromise.S3({
      apiVersion: '2006-03-01',
      region: region,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    });
    let bucket = config.s3BucketBase + region;
    try {
      debug('Creating S3 Bucket ' + bucket);
      let result = await s3.createBucket({
        Bucket: bucket,
        CreateBucketConfiguration: {
          LocationConstraint: region,
        },
        ACL: 'public-read',
      }).promise();
      console.dir(result.data);
      debug('hi');
    } catch (err) {
      if (err.code !== 'BucketAlreadyExists' && err.code !== 'BucketAlreadyOwnedByYou') {
        throw err;
      }
      debug('S3 Bucket already exists');
    }
    let backend = new storageBackend.S3Backend({
      region: region,
      bucket: bucket,
      urlTTL: 6400,
      sqs: sqs,
      s3: s3,
      memcached: memcached,
      allowedPatterns: [/.*/],
    });
    await backend.init();
    awsBackends.push(backend);
  }
  return {
    aws: awsBackends,
  }
}

setupBackends({
  s3BucketBase: `cloud-mirror-${process.env.NODE_ENV || 'development'}-`,
  awsRegions: ['us-west-2'],
}).then(backends => {
  console.log('Storage backends initialised');
  return Promise.all(backends.aws.map(x => x.startListeningToRequestQueue()));
}).then(console.dir, err => console.log(err.stack || err));

