let SQSConsumer = require('sqs-consumer');
let debugModule = require('debug');
let assert = require('assert');
let _ = require('lodash');

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
 * queueName:
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
 * maxReceiveCount (optional):
 *    The maximum number of times a message should fail to be processed before
 *    give up and dead lettering it
 * deadQueueName (optional):
 *    Name to give the dead letter queue.  Defaults to queueName + '_dead'
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
      'queueName', // SQS queue to listen on for copy requests
      'sqs', // SQS Instance to listen and publish on
      'batchSize', // Number of concurrent messages to process
      'handler', // Handler message for normal messages
    ]) {
      assert(typeof config[x] !== 'undefined', `QueueListener requires ${x} configuration value`);
      this[x] = config[x];
    }

    if (config.deadHandler) {
      this.deadHandler = config.deadHandler;
      this.deadBatchSize = config.deadBatchSize || config.batchSize;
    }

    this.maxReceiveCount = config.maxReceiveCount || 5;

    this.deadQueueName = config.deadQueueName || this.queueName + '_dead';
    this.debug = debugModule('cloud-mirror:queue:' + this.queueName);
  }

  /**
   * Declare required queue and dead letter queue, set up consumers
   */
  async init () {
    let awsRes = await this.sqs.createQueue({
      QueueName: this.deadQueueName,
    }).promise();
    this.deadQueueUrl = awsRes.data.QueueUrl;

    // Now we'll find the dead letter put queue's ARN
    awsRes = await this.sqs.getQueueAttributes({
      QueueUrl: this.deadQueueUrl,
      AttributeNames: ['QueueArn'],
    }).promise();
    this.deadQueueArn = awsRes.data.Attributes.QueueArn;

    // Now, we'll create the real queue
    this.debug('creating sqs put queue');
    awsRes = await this.sqs.createQueue({
      QueueName: this.queueName,
      Attributes: {
        RedrivePolicy: JSON.stringify({
          maxReceiveCount: this.maxReceiveCount,
          deadLetterTargetArn: this.deadQueueArn,
        }),
      },
    }).promise();

    this.queueUrl = awsRes.data.QueueUrl;

    this.debug({
      queueUrl: this.queueUrl,
      deadQueueUrl: this.deadQueueUrl,
    });

    // Create consumer for normal queue
    this.consumer = SQSConsumer.create({
      queueUrl: this.queueUrl,
      batchSize: this.batchSize,
      handleMessage: async (rawMsg, done) => {
        this.debug(`Recevied message for ${this.queueName}`);
        let msg;
        try {
          msg = JSON.parse(rawMsg.Body);
        } catch (err) {
          this.debug(`Failed to JSON.parse message for ${this.queueName}: ${rawMsg.Body}`); 
          return done(err);
        }

        try {
          await this.handler(msg);
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
        queueUrl: this.deadQueueUrl,
        batchSize: this.deadBatchSize,
        handleMessage: async (rawMsg, done) => {
          try {
            await this.deadHandler(rawMsg);
            done();
          } catch (err) {
            done(err);
          }
        },
        sqs: this.sqs,
      });
    }
   
  }

  start () {
    this.debug('listneing to put queue ' + this.queueUrl);
    this.consumer.start();
    if (this.deadConsumer) {
      this.deadConsumer.start();
    }
  }

  stop () {
    this.debug('no longer listneing to put queue ' + this.queueUrl);
    this.consumer.stop();
    if (this.deadConsumer) {
      this.deadConsumer.stop();
    }
  }

  async purge () {
    this.debug(`purging ${this.queueName}`);
    await this.sqs.purgeQueue({
      QueueUrl: this.queueUrl,
    }).promise();
  }

  async purgeDead () {
    this.debug(`purging dead queue for ${this.queueName} ${this.deadQueueName}`);
    await this.sqs.purgeQueue({
      QueueUrl: this.deadQueueUrl,
    }).promise();
  }

  async send (msg) {
    this.debug('sending message');
    if (typeof msg !== 'object') {
      throw new Error(`All messages sent to ${this.queueName} must be object, not ${typeof msg}`); 
    }

    let jsonMsg;
    try {
      jsonMsg = JSON.stringify(msg);
    } catch (err) {
      throw new Error(`All messages sent to ${this.queueName} must be JSON serializable`); 
    }

    let outcome = await this.sqs.sendMessage({
      QueueUrl: this.queueUrl,
      MessageBody: jsonMsg,
    }).promise();

    this.debug('sent message');
    return outcome;
  }

}

module.exports = {
  QueueManager,
};
