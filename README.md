# Cloud Mirror

This repository contains a tool which was developed by the Taskcluster team to
reduce inter region transfer costs in cloud object storage systems.  An example
of one of these storage systems is AWS S3.  In S3 it is free and fast to do an
S3 transfer within a given region, but costs a lot of money and takes a decent
amount of time if it does not exist in that region.  At the time of writing, we
use the `us-west-2` as our canonical storage region.

Cloud mirror is implemented as a very specialized proxy which copies objects
into a region the first time that a machine in that region requests that
resource.  This tool does not act as a fully conforming HTTP proxy and
conformance is not a goal.  This tool should only be used for immutable objects
which don't expire.

In the taskcluster implementation, we rewrite our urls using an algorithm which
roughly resembles (python-ish psuedocode):

```python
def rewriteUrl(inputUrl):
  service = determineService(inputUrl)
  region = determineRegion(service, inputUrl)
  base64Url = inputUrl.encode('base64')
  return 'http://cloud-mirror.taskcluster.net/v1/redirect/%s/%s/%s' % (service, region, base64Url)
```

# Overview of design

Cloud mirror is implemented as an API front end which serves redirects and back
ends.  The front and back ends communicate through SQS and use a redis cache.
It is important to note that redis is being used as a cache and not data
storage.  The most important front end method is the
`/v1/redirect/:service/:region/:base64(urL)` method.  This is the method that
the final consumer should use in place of the original url.  On receiving a
request for the `/redirect/` method, the front end will check if the redis
cache contains information on the requested resource in the specified region.

If the relevant information is not found in the redis cache, the front end will
calculate the url which the object store *would* use and do a `HEAD` request on
the object.  If the object is found to exist, the front end will determine how
much longer the object will exist in the backing store and backfill the redis
cache with this information so that future requests can skip this `HEAD`
request.  This is also how we deal with the redis cache being flushed: we'll
just backfill all resources at the low cost of an extra head request for each
object.

If the object is found in the redis cache, it will be stored with one of the
following statuses: `present`, `pending` or `error`.  `present` means that the
cache knows about the object and has the URL pointing to the backing object
ready to redirect a client to.  `pending` means that the cache knows about a
transfer being in progress.  It contains information on where the object *will*
show up eventually but should not be sent as a redirect to the client.  In the
pending state, the API front end will poll the cache key until the state
changes to `present`.  An optimization that we're considering is to use redis'
pub/sub features to monitor the keys instead of polling.

If a cache entry is in the `error` state, it means that the back end tried to
copy the file but failed.  A text copy of the stack trace from the failed copy
operation is also stored for easy access.  As currently implemented, the front
end does not redirect in the `error` state and instead retries the copy
operation as long as a request is alive using the standard polling interval.
Stack traces are not given to the client and instead kept private.  This is
done to lower the risk of sensitive data leaking.  A rough idea of what the
error is will be given to the client

When an object is requested to be copied into a region, a simple JSON message
is sent to an SQS queue for that given storage backend.  The API front end does
not do any actual copying itself.  A backend is a really simple system which
knows how to take an input URL and copy it into the object storage that the
backend represents.  The `CacheManager` class conatins all the logic for
managing the cache and has implementations for everything other than storage
system specific operations.  The `StorageProvider` class is a mostly-abstract
base class which should be completely implemented for each given storage
system.

# Contributing

Contributions are very welcome and we're happy.  Cloud-mirror is primiarily
developed by John Ford who can be found in `#taskcluster` on `irc.mozilla.org`
as `jhford`.  There are unit tests which should pass on your contribution as
well as an eslint configuration which declares the coding style for this
repository.  We use the `babel` javascript transformation toolkit to support
modern javascript.  Where possible, we use the `async` and `await` keywords to
make our async promises easier to work with.  This is strongly preferred in
contributions.  We also like to use `lodash` where javascript standards haven't
provided the needed utilities.

Running tests locally, requires a redis instance:
```sh
# This should do the trick
docker run --rm -p 6379:6379 redis
```

# taskcluster.net deployment notes

While this tool is designed to be general purpose, the taskcluster deployment
is the first and largest.  We use the docker cloud deployment tool to run the
entire system in the `us-west-2` region.  Docker cloud works with stack files
as the unit of configuration.  The stack file that we use has a load balancer,
a redis cache host, a front end and a back end.  The load balancer does all of
our SSL termination as well as load balancing.  Since the implementation we
have treats redis as a best-effort cache and not data storage, we have it
included in our stack file.  Both of these images are retagged copies of the
latest version from the tutum team.  We do not store the stack file in the
repository because it might contain secret data.  We hope that one day, the
docker cloud team will implement a feature which lets us use something to
redirect configuration values to a private values file.

# Troubleshooting and Deploying

Building a new version of the docker image for cloud-mirror should be
