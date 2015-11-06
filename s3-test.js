let aws = require('aws-sdk-promise');

let s3 = new aws.S3({
  apiVersion: '2006-03-01',
  region: 'us-west-2',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

try {
  s3.createBucket({Bucket: 'jhford-test'}).promise().then(function() {
    console.log('done');
  }).catch(function(err) {
    console.log(err.stack || err);
  });
} catch (e) {
  console.log(e.stack || e);
}
