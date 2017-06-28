# Cloud Sync Checker
Verify cloud sync propagation status.

Simple script to check if an AWS bucket has all of the objects returned by the
Swift listing. Note that due to the evetual consistency nature of Swift and S3,
the results may be stale. For example, a newly uploaded object may not be
returned by Swift when listing the container.

## Requirements

The scripts uses
[python-swiftclient](https://pypi.python.org/pypi/python-swiftclient) and
[boto3](https://pypi.python.org/pypi/boto3). Make sure these packages are
installed either in the virtual environment or globally.

## Usage

To use, invoke as follows:
```
verify_sync.py --auth-url <AUTH_URL> --account <AUTH_account> \
    --container <Container> --key <Swift account password> \
    --bucket <AWS bucket> --access-key <AWS Key> --secret <AWS Secret>
```

If you have SLOs (static large objects), you probably want to pass
`--check-slo`. This will cause a HEAD request on every object, but will validate
that the static large object has been properly uploaded as a Multipart Upload.

Dynamic large objects are not currently supported by cloud sync or by this
tool.
