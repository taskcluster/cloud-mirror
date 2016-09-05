/**
 * Return a promise that just waits a certain amoutn of time in ms
 */
function delayer(time) {
  return new Promise(resolve => {
    setTimeout(resolve, time);
  });
}

module.exports = delayer;
