let main = require('../lib/main');
let redis = require('redis');
let request = require('request-promise').defaults({
  //followRedirect: false,
  simple: false,
  gzip: true,
  resolveWithFullResponse: true,
});
let http = require('http');
let assume = require('assume');
let _aws = require('aws-sdk');
let _ = require('lodash');
let sinon = require('sinon');
let uuid = require('uuid');
let zlib = require('zlib');

let debug = require('debug')('s3-integration-tests');

let httpbin = 'https://taskcluster-httpbin.herokuapp.com';

let cm = require('../lib/cache-manager');
let sp = require('../lib/storage-provider');
let s3sp = require('../lib/s3-storage-provider');

async function emptyBucket(awsCfg, bucket) {
  debug('emptying test bucket');

  let aws = new _aws.S3(_.omit(awsCfg, 'region'));

  let x;
  try {
    let y = await aws.headBucket({Bucket: bucket}).promise();
    if (Object.keys(y.data).length === 0) {
      y = false;
    } else {
      y = true;
    }
  } catch (e) {
    x = false;
  }
  if (!x) {
    debug('bucket absent, nothing to delete');
    return;
  }

  let objs = await aws.listObjects({Bucket: bucket}).promise();

  objs = objs.data.Contents.map(x => {
    return {
      Key: x.Key,
    };
  });

  objs = {
    Bucket: bucket,
    Delete: {
      Objects: objs,
    },
  };

  await aws.deleteObjects(objs).promise();
  debug('finished emptying test bucket');
}

describe('Integration Tests', () => {

  let redis;
  let baseUrl;
  let cfg;
  let sandbox;
  let queue;
  let queueListenerFactory;
  let cacheManager;
  let backend;
  let server;

  before(async () => {
    cfg = await main('cfg', {process: 'cfg', profile: 'test'});

    queueListenerFactory = await main('queueListenerFactory', {
      process: 'queueListenerFactory',
      profile: 'test',
    });

    queue = await queueListenerFactory();

    let cacheManagers = await main('cacheManagers', {
      process: 'cacheManager',
      profile: 'test',
    });

    let x = cacheManagers.filter(x => {
      return x.id === 's3_us-west-1';
    });

    assume(x.length).equals(1);
    cacheManager = x[0];
    assume(cacheManager).is.ok();

    server = await main('server', {
      process: 'server',
      profile: 'test',
    });

    baseUrl = cfg.server.publicUrl + '/v1';
  });

  beforeEach(async () => {
    sandbox = sinon.sandbox.create();
    redis = await main('redis', {process: 'redis', profile: 'test'});
    await redis.flushdb();
    await emptyBucket(cfg.aws, cacheManager.storageProvider.bucket);
    queue.start();
  });

  afterEach(async () => {
    queue.stop();
    sandbox.restore();
  });

  after(async () => {
    //await emptyBucket(cfg.aws, cacheManager.storageProvider.bucket);
  });

  function assertRedirected(expected, actual) {
    let realBucket = cacheManager.storageProvider.bucket;
    let redirectedUrl = `https://${realBucket}.s3-us-west-1.amazonaws.com/`;
    redirectedUrl += encodeURIComponent(expected);
    assume(redirectedUrl).equals(actual);
  }

  // Corresponding negative for the positive check
  function assertNotRedirected(expected, actual) {
    assume(expected).equals(actual);
  }

  function testRedirect(name, testUrl, shouldRedirect = true) {
    it(name, async () => {
      let expectedRedirect = baseUrl + '/redirect/s3/us-west-1/';
      expectedRedirect += encodeURIComponent(testUrl);

      // Request the response from cloud-mirror so we can see which
      // URL it's going to redirect us to
      let actual = await request({
        url: expectedRedirect,
        followRedirect: false,
        simple: false,
        resolveWithFullResponse: true,
      });

      assume(actual.statusCode).equals(302);
      let assertFunc = shouldRedirect ? assertRedirected : assertNotRedirected;
      assertFunc(testUrl, actual.headers.location);
      let bodyJson = JSON.parse(actual.body);
      assume(bodyJson.status).equals('present');

      // Check that we get the same response body for each call
      let expected = await request(testUrl, {
        headers: {
          'Accept-Encoding': '*',
          'User-Agent': 'cloud-mirror/1.0.0',
        },
      });
      let actual1 = await request(expectedRedirect);
      let actual2 = await request(expectedRedirect);

      if (expected.headers['content-type'] === 'application/json') {
        let expectedBody;
        let actual1Body;
        let actual2Body;

        // Heroku sadly puts in a useless (to us) header which changes on each
        // request.  Ideally we'd use the same .data event to get the data from
        // the actual request made by the copy process, but this is a little
        // bit of work so let's be lazy and account for the single header
        // that's different between requests
        let badHeaders = ['X-Request-Id', 'Connect-Time', 'Total-Route-Time'];
        if (expected.headers['transfer-encoding'] !== 'chunked') {
          // Non-chunked encoded messages from httpbin are easy
          expectedBody = JSON.parse(expected.body);
          actual1Body = JSON.parse(actual1.body);
          actual2Body = JSON.parse(actual2.body);
          for (let t of [expectedBody, actual1Body, actual2Body]) {
            for (let header of badHeaders) {
              if (t.headers && t.headers[header]) {
                delete t.headers[header];
              }
            }
          }
        } else {
          // But chunked encoded messages are hard
          let fix = (q) => {
            q = q.replace(/\n/g, ',');
            if (q.charAt(q.length - 1)) {
              q = q.slice(0, q.length - 1);
            }
            q = '[' + q + ']';
            q = JSON.parse(q);
            return q;
          };

          expectedBody = fix(expected.body);
          actual1Body = fix(actual1.body);
          actual2Body = fix(actual2.body);

          for (let r of [expectedBody, actual1Body, actual2Body]) {
            for (let m of r) {
              for (let header of badHeaders) {
                if (m.headers && m.headers[header]) {
                  delete m.headers[header];
                }
              }
            }
          }
        }
        // Here's the end of the stuff for dealing with the Heroku headers

        assume(actual1Body).deeply.equals(expectedBody);
        assume(actual2Body).deeply.equals(expectedBody);
      } else {
        assume(expected.body).equals(actual1.body);
        assume(expected.body).equals(actual2.body);
      }

    });
  }

  // Simple redirect cases for different content types but for
  // expected behaviour
  testRedirect('should cache a simple resource', httpbin + '/ip');
  testRedirect('should cache a gzip encoded resource', httpbin + '/gzip');
  testRedirect('should cache a deflate encoded resource', httpbin + '/deflate');
  testRedirect('should cache a utf8 encoded resource', httpbin + '/encoding/utf8');
  testRedirect('should cache a simple streamed resource', httpbin + '/stream/5');
  testRedirect('should cache a byte-stream resource', httpbin + '/stream-bytes/2000?seed=1234&chunk_size=10');

  function testFailure(name, testUrl, expectedStatusCode) {
    it(name, async () => {
      let expectedRedirect = baseUrl + '/redirect/s3/us-west-1/';
      expectedRedirect += encodeURIComponent(testUrl);

      let actual = await request({
        url: expectedRedirect,
        followRedirect: false,
        simple: false,
        resolveWithFullResponse: true,
      });

      assume(actual.statusCode).equals(expectedStatusCode);
    });
  }

  // For all error states which are unknown, make sure we return a 503 to
  // allow clients to retry
  testFailure('should not cache a 400', httpbin + '/status/400', 400);
  testFailure('should not cache a 401', httpbin + '/status/401', 401);
  testFailure('should not cache a 403', httpbin + '/status/403', 403);
  testFailure('should not cache a 404', httpbin + '/status/404', 404);
  testFailure('should not cache a 500', httpbin + '/status/500', 500);
  testFailure('should not cache a 503', httpbin + '/status/503', 503);

  // For cases where the request is not allowed, return 403
  testFailure('should only cache whitelisted url', 'https://www.facebook.com', 403);
  testFailure('should only cache ssl url, even if whitelisted', 'http://www.google.com', 403);

  it('should use the storage providers url and not the original one', async () => {
    let testUrl = httpbin + '/html';
    let fakeUrl = 'https://www.google.com';
    let fakeGetBackendUrl = sandbox.stub(cm.CacheManager.prototype, 'getUrlForRedirect');
    fakeGetBackendUrl.returns(Promise.resolve({
      status: 'present',
      url: fakeUrl,
    }));

    let urlEncodedTestUrl = encodeURIComponent(testUrl);
    let actual = await request({
      url: baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl,
      followRedirect: false,
      simple: false,
      resolveWithFullResponse: true,
    });

    assume(actual.statusCode).equals(302);
    assume(actual.headers['location']).equals(fakeUrl);
  });

  it('should backfill cache', async () => {
    let testUrl = httpbin + '/user-agent';
    let urlEncodedTestUrl = encodeURIComponent(testUrl);
    let key = cacheManager.cacheKey(testUrl);
    let expected = await request(baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl);
    let origVal = await redis.hgetallAsync(key);
    await redis.delAsync(key);
    let actual = await request(baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl);
    let afterVal = await redis.hgetallAsync(key);
    assume(actual.body).equals(expected.body);
    assume(afterVal).deeply.equals(origVal);
  });

  it('should redirect to original url if caching takes too long', async () => {
    let testUrl = httpbin + '/user-agent';
    let fakeGetBackendUrl = sandbox.stub(cm.CacheManager.prototype, 'getUrlForRedirect');
    fakeGetBackendUrl.returns(Promise.resolve({
      status: 'pending',
      url: testUrl,
    }));
    let urlEncodedTestUrl = encodeURIComponent(testUrl);
    let actual = await request({
      url: baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl,
      followRedirect: false,
      simple: false,
      resolveWithFullResponse: true,
    });

    assume(actual.statusCode).equals(302);
    assume(actual.headers['location']).equals(testUrl);
  });

  it('should redirect to original url if there is an error while caching', async () => {
    let testUrl = httpbin + '/user-agent';
    let fakeGetBackendUrl = sandbox.stub(cm.CacheManager.prototype, 'getUrlForRedirect');
    fakeGetBackendUrl.returns(Promise.resolve({
      status: 'error',
      url: testUrl,
    }));
    let urlEncodedTestUrl = encodeURIComponent(testUrl);
    let actual = await request({
      url: baseUrl + '/redirect/s3/us-west-1/' + urlEncodedTestUrl,
      followRedirect: false,
      simple: false,
      resolveWithFullResponse: true,
    });

    assume(actual.statusCode).equals(302);
    assume(actual.headers['location']).equals(testUrl);
  });

  it('should purge correctly', async () => {
    let testUrl = httpbin + '/bytes/1024';
    let expectedRedirect = baseUrl + '/redirect/s3/us-west-1/';
    expectedRedirect += encodeURIComponent(testUrl);
    let purgeUrl = baseUrl + '/purge/s3/us-west-1/';
    purgeUrl += encodeURIComponent(testUrl);

    // Request the response from cloud-mirror so we can see which
    // URL it's going to redirect us to
    let actual = await request({
      url: expectedRedirect,
      followRedirect: false,
      simple: false,
      resolveWithFullResponse: true,
    });

    assume(actual.statusCode).equals(302);
    assertRedirected(testUrl, actual.headers.location);
    let bodyJson = JSON.parse(actual.body);
    assume(bodyJson.status).equals('present');

    let beforePurge1 = await request(expectedRedirect);
    let beforePurge2 = await request(expectedRedirect);

    let purge = await request({
      url: purgeUrl,
      followRedirect: false,
      simple: false,
      resolveWithFullResponse: true,
      method: 'DELETE',
    });

    assume(purge.statusCode).equals(204);

    let afterPurge = await request(expectedRedirect);

    assume(beforePurge1.body).equals(beforePurge2.body);
    assume(afterPurge.body).not.equals(beforePurge1.body);

  });
});
