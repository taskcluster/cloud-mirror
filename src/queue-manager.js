let SQSConsumer = require('sqs-consumer');
let debug = require('debug')('cloud-mirror:queue');
let assert = require('assert');
let _ = require('lodash');
let slugid = require('slugid');


/**
 * Initialize a queue
 * 
 * Options:
 *  - sqs: instance of an SQS client with which to perform operations
 *  - queueName: human readable name of the main queue to consume from and insert into
 *  - maxReceiveCount (optional): maximum number of times to try processing this message
 *    before giving up
 *  - deadLetterSuffix (optional): append this suffix to the name of the normal work queue
 *    to determine the name of the dead letter queue.
 *
 * Returns:
 *  {
 *    queueUrl: '...',
      deadQueueUrl: '...'
 *  }
 */
async function initQueue (sqs, queueName, maxReceiveCount = 5, deadLetterSuffix = '_dead') {
  assert(typeof sqs === 'object', 'Must provide SQS instance as object');
  assert(typeof queueName === 'string', 'Must provide queueName as string');
  assert(typeof maxReceiveCount === 'number', 'Must provide maxReceiveCount as number');
  assert(typeof deadLetterSuffix === 'string', 'Must provide deadLetterSuffix as string');

  let awsRes;
  let queueUrl;
  let deadQueueUrl;
  let deadQueueName = queueName + deadLetterSuffix;

  // We need to create the dead letter queue first because we need to query the ARN
  // of the queue so that we can set it when creating the main queue
  awsRes = await sqs.createQueue({
    QueueName: deadQueueName,
  }).promise();
  deadQueueUrl = awsRes.data.QueueUrl;
  debug(`created dead queue: ${deadQueueUrl}`);

    // Now we'll find the dead letter put queue's ARN
  awsRes = await sqs.getQueueAttributes({
    QueueUrl: deadQueueUrl,
    AttributeNames: ['QueueArn'],
  }).promise();
  let deadQueueArn = awsRes.data.Attributes.QueueArn;

  awsRes = await sqs.createQueue({
    QueueName: queueName,
    Attributes: {
      VisibilityTimeout: '1000',
      RedrivePolicy: JSON.stringify({
        maxReceiveCount: maxReceiveCount,
        deadLetterTargetArn: deadQueueArn,
      }),
    },
  }).promise();

  queueUrl = awsRes.data.QueueUrl;

  debug(`created queue: ${queueUrl}`);

  return {queueUrl, deadQueueUrl};
}

/**
 * A queue manager wraps SQS in some convenience methods and a dead letter
 * queue by default.  A QueueManager manages a specific queue and its
 * associated dead letter queue.  A handler for the Queue should be a promise
 * returning function which takes a Javascript object that represents the
 * message.  All messages are assumed to be JSON.  A dead letter handler can
 * optionally be provided which will take the raw message body from the dead
 * letter queue.  The reason that this handler does not take a Javascript
 * object is that it might have been a JSON parsing error that caused the
 * rejection in the first place.
 * 
 * Parameters for this class are as follows:
 * queueUrl:
 *    This is the friendly name for the queue that's being managed.  It will be
 *    used to declare the queues required.  This will be a component of the ARN
 *    and the URL for the queue.  The dead queue will always be this value with
 *    '_dead' appended
 * sqs:
 *    This is an instances of an aws-sdk.SQS that will be used for all queue
 *    operations.
 * batchSize:
 *    This is the number of messages that should be processed concurrently by
 *    this QueueManager
 * handler:
 *    This is a promise returning function which is given a Javascript object
 *    reprsenting the parsed JSON message body.  The callback from the SQS
 *    message will be called with no value on success and with the rejection
 *    value on failure.
 * deadQueueUrl (optional): URL to the dead letter queue
 * deadHandler (optional):
 *    Like the handler parameter, except for the dead letter queue and does not
 *    attempt to parse the message in any way at all.  If an error occurs, the
 *    message is dropped.
 * deadBatchSize (optional):
 *    If provided, it will be the batch size for the dead letter handler.  If 
 *    not provided, the normal message batch size will be used instead.
 */
class QueueManager {
  constructor (config) {
    for (let x of [
      'queueUrl', // SQS Queue URL to listen to
      'sqs', // SQS Instance to listen and publish on
      'batchSize', // Number of concurrent messages to process
      'handler', // Handler message for normal messages
    ]) {
      assert(typeof config[x] !== 'undefined', `QueueListener requires ${x} configuration value`);
      this[x] = config[x];
    }

    if (config.deadHandler) {
      assert(config.deadQueueUrl, 'When using a dead letter queue, must provide url');
      this.deadHandler = config.deadHandler;
      this.deadQueueUrl = config.deadQueueUrl;
      this.deadBatchSize = config.deadBatchSize || config.batchSize;
    }

    this.qid = slugid.nice();

    let that = this;

    // Create consumer for normal queue
    this.consumer = SQSConsumer.create({
      queueUrl: that.queueUrl,
      batchSize: that.batchSize,
      handleMessage: async (rawMsg, done) => {
        debug(`Recevied message on ${that.queueUrl} ${that.qid}`);
        let msg;
        try {
          msg = JSON.parse(rawMsg.Body);
        } catch (err) {
          debug(`Failed to JSON.parse message on ${that.queueUrl} ${that.qid}: ${rawMsg.Body}`); 
          return done(err);
        }

        try {
          debug('about to run handler');
          await that.handler(msg);
          debug('ran handler');
          done();
        } catch (err) {
          done(err);
        }
      },
      sqs: this.sqs,
    });
   
    // Create consumer for dead letter queue
    if (this.deadHandler) {
      this.deadConsumer = SQSConsumer.create({
        queueUrl: that.deadQueueUrl,
        batchSize: that.deadBatchSize,
        handleMessage: async (rawMsg, done) => {
          try {
            await that.deadHandler(rawMsg);
            done();
          } catch (err) {
            done(err);
          }
        },
        sqs: that.sqs,
      });
    }
   
  }

  start () {
    debug(`listneing to put queue ${this.queueUrl} ${this.qid}`);
    this.consumer.start();
    if (this.deadConsumer) {
      this.deadConsumer.start();
    }
  }

  stop () {
    debug(`no longer listneing to put queue ${this.queueUrl} ${this.qid}`);
    this.consumer.stop();
    if (this.deadConsumer) {
      this.deadConsumer.stop();
    }
  }

  async purge () {
    debug(`purging ${this.queueUrl} ${this.qid}`);
    await this.sqs.purgeQueue({
      QueueUrl: this.queueUrl,
    }).promise();
  }

  async purgeDead () {
    debug(`purging dead queue for ${this.queueUrl} ${this.deadQueueUrl} ${this.qid}`);
    await this.sqs.purgeQueue({
      QueueUrl: this.deadQueueUrl,
    }).promise();
  }

  async send (msg) {
    debug(`sending message to ${this.queueUrl} ${this.qid}`);
    if (typeof msg !== 'object') {
      throw new Error(`All messages sent to ${this.queueUrl} must be object, not ${typeof msg}`); 
    }

    let jsonMsg;
    try {
      jsonMsg = JSON.stringify(msg);
    } catch (err) {
      throw new Error(`All messages sent to ${this.queueUrl} ${this.qid} must be JSON serializable`); 
    }

    let outcome = await this.sqs.sendMessage({
      QueueUrl: this.queueUrl,
      MessageBody: jsonMsg,
    }).promise();

    debug('sent message');
    return outcome;
  }

}

module.exports = {
  QueueManager,
  initQueue,
};
