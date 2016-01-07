let base = require('taskcluster-base');

let exchanges = new base.Exchanges({
  title: 'S3 Distribute Pulse Exchanges',
  description: [
    'Exchanges from the s3 distribution manager',
  ].join('\n'),
  schemaPrefix: 'http://schemas.taskcluster.net/aws-provisioner/v1/',
});

module.exports = exchanges;

let commonRoutingKey = [
  {
    // Let's keep the "primary." prefix, so we can support custom routing keys
    // in the future, I don't see a need for it here. But it's nice to have the
    // option of adding it later...
    name: 'routingKeyKind',
    summary: 'Identifier for the routing-key kind. This is ' +
             'always `\'primary\'` for the formalized routing key.',
    constant: 'primary',
    required: true,
  }, {
    name: 'service',
    summary: 'service that the resource is requested in',
    required: true,
    maxSize: 22,
  }, {
  }, {
    name: 'region',
    summary: 'region that the resource is requested in',
    required: true,
    maxSize: 22,
  }, {
    name: 'reserved',
    summary: 'Space reserved for future routing-key entries, you ' +
             'should always match this entry with "#". As ' +
             'automatically done by our tooling, if not specified.',
    multipleWords: true,
    maxSize: 1,
  },
];

/** Build an pulse compatible message from a message */
let commonMessageBuilder = function (message) {
  message.version = 1;
  return message;
};

/** Build a routing-key from message */
let commonRoutingKeyBuilder = function (message) {
  return {
    service: message.service,
    region: message.region,
  };
};

/** Build a list of routes to CC */
let commonCCBuilder = function () {
  return [];
};

exchanges.declare({
  exchange: 'resource-requested',
  name: 'resourceRequested',  // Method to call on publisher
  title: 'Resource Requested Message',
  description: [
    'Sent when a resource is requested'
  ].join('\n'),
  routingKey: commonRoutingKey,
  schema: 'resource.json#',
  messageBuilder: commonMessageBuilder,
  routingKeyBuilder: commonRoutingKeyBuilder,
  CCBuilder: commonCCBuilder,
});
