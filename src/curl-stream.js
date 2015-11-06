let Readable = require('stream').Readable;
let Curl = require('node-libcurl').Curl;

// https://github.com/JCMais/node-libcurl/blob/develop/examples/progress-callback.js
class CurlStream extends Readable {
  constructor(url, opts) {
    super(opts);
    let c = this.curl = new Curl();
    c.setOpt('URL', url);
    c.setOpt(Curl.option.NOPROGRESS, false);
    // NOT SECURE!!! I should instead do HEAD on the URL
    // and see if the Location: header is set to the same
    // protocol as the original request
    // see http://curl.haxx.se/mail/lib-2007-10/0025.html
    c.setOpt(Curl.option.FOLLOWLOCATION, true); 
    c.enable(Curl.feature.NO_STORAGE);
    c.setProgressCallback((dltotal, dlnow) => {
      console.log(`dltotal: ${dltotal}, dlnow: ${dlnow}`);
      // Not sure if this is required but the example file
      // always has it, so including it here
      return 0;
    });

    c.onHeader = (chunk) => {
      console.log('Header chunk: ' + chunk);
      return chunk.length
    }

    c.onData = (chunk) => {
      console.log('Data chunk: ' + chunk);
      return chunk.length;
    };

    c.on('end', () => {
      this.push(null);
      c.close();
    });

    c.on('error', err => {
      this.emit('err', err);
    });
  }

  _read(size) {

  }
}

module.exports = CurlStream;
