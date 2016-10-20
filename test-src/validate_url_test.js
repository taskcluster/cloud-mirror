let subject = require('../lib/validate-url');
let assume = require('assume');

describe('url following and validation', () => {
  it('should follow Urls and validate them', async () => {
    let result = await subject('https://taskcluster-httpbin.herokuapp.com/redirect/6');
    console.dir(result);
    assume(result.addresses.length).equals(7);
  });

  it('should complain about too many redirects', async () => {
    let result = await subject('https://taskcluster-httpbin.herokuapp.com/redirect/32');
    if (result !== false) {
      throw new Error();
    }
  });

  it('should complain about insecure urls', async () => {
    let result = await subject('http://taskcluster-httpbin.herokuapp.com/redirect/32');
    if (result !== false) {
      throw new Error();
    }
  });

  it('should complain about non-matching urls', async () => {
    let result = await subject('http://taskcluster-httpbin.herokuapp.com/redirect/32', [/john/]);
    if (result !== false) {
      throw new Error();
    }
  });

});
