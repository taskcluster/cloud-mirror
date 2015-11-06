let aws = require('aws-sdk');

let s3 = new aws.S3({
  apiVersion: '2006-03-01',
  region: 'us-west-2',
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

s3.createBucket({Bucket: 'jhford-test'}, function(err, data) {
  if (err) {
    console.log(err.stack || err);
  } else {
    console.dir(data);
  }
});
