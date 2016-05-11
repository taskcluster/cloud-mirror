let main = require('../lib/main');
let assume = require('assume');
let _ = require('lodash');
let sinon = require('sinon');
let uuid = require('uuid');

let debug = require('debug')('sqs-integration-tests');

let subject = require('../lib/queue-manager').QueueManager;

/**
 * NOTE:
 *    Only a single purge per-queue can happen every 60 seconds and without the
 *    purges, the tests are janky.  Sucks, but that's life
 */

describe('Integration Tests', () => {
  let sandbox;
  let cfg;
  let sqs;
  let q;

  before(async () => {
    cfg = await main('cfg', {process: 'cfg', profile: 'test'});
    sqs = await main('sqs', {process: 'sqs', profile: 'test'});
  });

  beforeEach(async () => {
    sandbox = sinon.sandbox.create();
  });

  afterEach(async () => {
    if (q) {
      q.stopListeningToQueue();
    }
    sandbox.restore();
  });

  it('should be able to send and receive message', async function(done) {
    let expected = {uuid: uuid.v4()};

    let q = new subject({
      queueName: 'testingqueue1',
      sqs: sqs,
      batchSize: 1,
      maxReceiveCount: 1,
      handler: async (obj) => {
        try {
          assume(obj.uuid).equals(expected.uuid);
          q.stop();
          done();
        } catch (err) {
          q.stop();
          done(err);
        }
      },
    });

    try {
      await q.init();
      await q.purge();
      q.start();
      await q.send(expected);
    } catch (err) {
      q.stop();
      done(err);
    }
  });
  
  it('should dead letter messages when the handler fails', async function(done) {
    let expected = {uuid: uuid.v4()};

    let q = new subject({
      queueName: 'testingqueue2',
      sqs: sqs,
      batchSize: 1,
      maxReceiveCount: 1,
      handler: async (obj) => {
        assume(obj.uuid).equals(expected.uuid);
        console.log('failing now');
        throw new Error();
      },
      deadHandler: async (obj) => {
        q.stop();
        done();
      },
    });

    try {
      await q.init();
      await q.purge();
      await q.purgeDead();
      q.start();
      await q.send(expected);
    } catch (err) {
      q.stop();
      done(err);
    }
  });
  
  it('should refuse to send messages with encoding problems', async function(done) {
    let expected = '{\'not json\'}';

    let q = new subject({
      queueName: 'testingqueue3',
      sqs: sqs,
      batchSize: 1,
      maxReceiveCount: 1,
      handler: async (obj) => {
        done(new Error('Shouldnt even be sent'));
      },
      deadHandler: async (obj) => {
        done(new Error('Shouldnt even be sent'));
      },
    });

    try {
      await q.init();
      await q.purge();
      await q.purgeDead();
      await q.send(expected);
      done(new Error('Shouldnt even be sent'));
    } catch (err) {
      done();
    }
  });

  it('should refuse to process messages with encoding problems', async function(done) {
    let expected = '{\'not json\'}';

    let q = new subject({
      queueName: 'testingqueue4',
      sqs: sqs,
      batchSize: 1,
      maxReceiveCount: 1,
      handler: async (obj) => {
        q.stop();
        done(new Error('this code shouldnt run because its an encoding problem'));
      },
      deadHandler: async (obj) => {
        try {
          assume(obj).equals(expected);
          q.stop();
          done();
        } catch (err) {
          q.stop();
          done(err);
        }
      },
    });
    try {
      await q.init();
      await q.purge();
      await q.purgeDead();
      q.start();

      await sqs.sendMessage({
        QueueUrl: q.queueUrl,
        MessageBody: expected, 
      }).promise();
    } catch (err) {
      q.stop();
      done(err);
    }
  });
});
