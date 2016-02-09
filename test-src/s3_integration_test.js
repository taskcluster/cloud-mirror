"use strict";

let main = require('../lib/main');
let redis = require('redis');
let request = require('request-promise');
let assume = require('assume');
let zlib = require('zlib');
let _aws = require('aws-sdk-promise');
let _ = require('lodash');
let sinon = require('sinon');

let debug = require('debug')('s3-integration-tests');

let httpbin = 'https://httpbin.org';

async function deleteBucketRecursively(cfg) {
  debug('Deleting test bucket');

  let aws = new _aws.S3(_.omit(cfg.aws, 'region'));
  let testBucket = cfg.backend.s3.bucketBase + cfg.server.env + '-us-west-1';

  let x;
  try {
    let y = await aws.headBucket({ Bucket: testBucket}).promise();
    if (Object.keys(y.data).length === 0) {
      y = false;
    } else {
      y = true;
    }
  } catch (e) {
    x = false;
  }
  if (!x) {
    debug('Bucket already gone, nothing to delete');
    return
  }

  let objs = await aws.listObjects({ Bucket: testBucket}).promise();

  objs = objs.data.Contents.map(x => {
    return {
      Key: x.Key,
    };
  });

  objs = {
    Bucket: testBucket,
    Delete: {
      Objects: objs,  
    },
  };

  await aws.deleteObjects(objs).promise();
  await aws.deleteBucket({Bucket: testBucket}).promise();
  debug('Finished deleting test bucket');
}

describe('Integration Tests', function() {

  let redis;
  let baseUrl;
  let cfg;
  let testBucket;

  before(async () => {
    cfg = await main('cfg', {process: 'cfg', profile: 'test'});
    await deleteBucketRecursively(cfg);
  });

  beforeEach(async () => {
    redis = await main('redis', {process: 'redis', profile: 'test'});
    baseUrl = cfg.server.publicUrl + '/v1';
    await redis.flushdb();
  });

  afterEach(async () => {
    await deleteBucketRecursively(cfg);
  });

  it('should be able to start api server', async function() {
    let server = await main('server', {
      process: 'server',
      profile: 'test',
    });
    return server.terminate();
  });

  it('should be able to start and stop listening backends', async function() {
    let backends = await main('listeningS3Backends', {
      process: 'listeningS3Backends',
      profile: 'test',
    });

    await backends['us-west-1'].stopListeningToRequestQueue();
  });


  describe('functions', function() {
    let server;
    let backend;

    before(async () => {
      server = await main('server', {
        process: 'server',
        profile: 'test',
      });
      backend = await main('listeningS3Backends', {
        process: 'server',
        profile: 'test',
      });
      backend = backend['us-west-1'];
    });

    after(async () => {
      server.terminate();
      await backend.stop
    });

    beforeEach(async () => {
      await backend.startListeningToRequestQueue();
    });


    it('should cache a url', async () => {
      let testUrl = httpbin + '/ip';
      let expected = await request(testUrl);
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(JSON.parse(actual1)).deeply.equals(JSON.parse(expected));
      assume(JSON.parse(actual2)).deeply.equals(JSON.parse(actual1));
    });

    it('should cache a gzip url', async () => {
      let testUrl = httpbin + '/gzip';
      let expected = await request(testUrl);
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(zlib.gunzip(actual1)).deeply.equals(zlib.gunzip(expected));
      assume(zlib.gunzip(actual2)).deeply.equals(zlib.gunzip(actual1));
    });

    it('should cache a deflate url', async () => {
      let testUrl = httpbin + '/deflate';
      let expected = await request(testUrl);
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(zlib.inflate(actual1)).deeply.equals(zlib.inflate(expected));
      assume(zlib.inflate(actual2)).deeply.equals(zlib.inflate(actual1));
    });

    it('should cache a utf-8 encoded url', async () => {
      let testUrl = httpbin + '/encoding/utf8';
      let expected = await request(testUrl);
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(actual1).deeply.equals(expected);
      assume(actual2).deeply.equals(actual1);
    });

    it('should cache streamed url', async () => {
      let testUrl = httpbin + '/stream/200';
      let expected = await request(testUrl, {headers: { 'Accept-Encoding': '*' }});
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(actual1).deeply.equals(expected);
      assume(actual2).deeply.equals(actual1);
    });

    it('should cache streamed-bytes url', async () => {
      let testUrl = httpbin + '/stream-bytes/2000?seed=1234&chunk_size=10';
      let expected = await request(testUrl, {headers: { 'Accept-Encoding': '*' }});
      let actual1 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      let actual2 = await request(baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl));
      assume(actual1).deeply.equals(expected);
      assume(actual2).deeply.equals(actual1);
    });

    it('should backfill cache', async () => {
      let testUrl = httpbin + '/user-agent';
      let urlEncodedTestUrl = encodeURIComponent(testUrl);
      let key = backend.id + '_' + urlEncodedTestUrl;
      let expected = await request(baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl);
      let origVal = await redis.hgetallAsync(key);
      await redis.delAsync(key);
      let actual = await request(baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl);
      let afterVal = await redis.hgetallAsync(key);
      assume(actual).deeply.equals(expected);
      assume(afterVal).deeply.equals(origVal);
    });

    it('should redirect to original url after timeout', async done => {
      await backend.stopListeningToRequestQueue();
      let testUrl = 'https://www.mozilla.org';
      try {
        let expected = await request({
          url: baseUrl + '/redirect/s3/us-west-1/' + encodeURIComponent(testUrl),
          followRedirect: false
        });
        done(new Error('did not redirect back to original url')); 
      } catch (e) {
        assume(e.url === testUrl);
        done();
      }
    });
  });
});
