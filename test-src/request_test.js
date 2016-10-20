let assume = require('assume');
let subject = require('../lib/request');
let fs = require('fs');
let memstream = require('memory-streams');

let httpBin = 'http://taskcluster-httpbin.herokuapp.com';
let httpsBin = 'https://taskcluster-httpbin.herokuapp.com';
// Sometimes, It's useful to run nc -l 1080 on your machine and see *exactly*
// what is being sent
//httpBin = 'http://localhost:1080';

describe('request', () => {
  it('should complete an HTTP GET request', async () => {
    let response = await subject.request(httpBin + '/user-agent', {
      allowUnsafeUrls: true,
    });
    // Heh, this will likely change if they change their server, but then we'll
    // just need to change this field.
    assume(response.headers.server).is.OK;
  });
  
  it('should complete an HTTPS GET request', async () => {
    let response = await subject.request(httpsBin + '/user-agent');
    // Heh, this will likely change if they change their server, but then we'll
    // just need to change this field.
    assume(response.headers.server).is.OK;
  });

  it('should complete a streamed POST request', async () => {
    return new Promise(async (res, rej) => {
      try {
        let reader = fs.createReadStream(require.resolve('../package.json'));
        let writer = new memstream.WritableStream();

        reader.on('error', rej);
        writer.on('error', rej);

        let response = await subject.request(httpBin + '/post', {
          method: 'post',
          stream: reader,
          contentType: 'application/json',
          allowUnsafeUrls: true,
        });

        assume(response.headers.server).is.OK;

        response.pipe(writer);

        response.on('end', () => {
          // NOTE: There is a bug in httpbin 
          // https://github.com/Runscope/httpbin/issues/102
          // that causes streamed responses not to work properly
          // which means that we don't get an echo-back of data
          res();
        });
      } catch (err) {
        rej(err);
      }
    });
  });

  it('should complete a non-streamed POST request', async () => {
    return new Promise(async (res, rej) => {
      try {
        //let data = fs.readFileSync(require.resolve('../package.json'));
        let writer = new memstream.WritableStream();

        writer.on('error', rej);

        let response = await subject.request(httpBin + '/post', {
          method: 'post',
          data: new Buffer('john!'),
          contentType: 'application/json',
          allowUnsafeUrls: true,
        });

        assume(response.headers.server).is.OK;

        response.pipe(writer);

        response.on('end', () => {
          let data = JSON.parse(writer.toBuffer());
          assume(data.data).equals('john!');
          res();
        });
      } catch (err) {
        rej(err);
      }
    });
  });
});
