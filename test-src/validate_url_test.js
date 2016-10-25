let subject = require('../lib/validate-url');
let assume = require('assume');

describe('url following and validation', () => {
  it('should follow Urls and validate them', async () => {
    let result = await subject({
      url: 'https://taskcluster-httpbin.herokuapp.com/redirect/6',
      allowedPatterns: [/.*/],
      ensureSSL: true,
    });
    console.dir(result);
    assume(result.addresses.length).equals(7);
  });

  // TODO: make sure that the caught errors have a message that's expected

  it('should complain about too many redirects', async () => {
    try {
      let result = await subject({
        url: 'https://taskcluster-httpbin.herokuapp.com/redirect/100',
        allowedPatterns: [/.*/],
        ensureSSL: true,
      });
      return Promse.reject(new Error());
    } catch (err) { }
  });

  it('should complain about insecure urls', async () => {
    try {
      let result = await subject({
        url: 'http://taskcluster-httpbin.herokuapp.com/redirect/2',
        allowedPatterns: [/.*/],
        ensureSSL: true,
      });
      return Promse.reject(new Error());
    } catch (err) { }
  });

  it('should complain about non-matching urls', async () => {
    try {
      let result = await subject({
        url: 'http://taskcluster-httpbin.herokuapp.com/redirect/2',
        allowedPatterns: [/^noturl$/],
        ensureSSL: true,
      });
      return Promse.reject(new Error());
    } catch (err) { }
  });

});
