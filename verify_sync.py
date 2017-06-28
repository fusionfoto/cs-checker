#!/usr/bin/env python

import argparse
import boto3
import hashlib
import json
import swiftclient

class ListStore(object):
    def __init__(self, conn, args):
        self.conn = conn
        self.args = args
        self.index = 0
        self.results = None

    def list_next(self):
        raise NotImplementedError

    def process_entry(self, entry):
        return entry

    def next(self):
        if self.results is None or len(self.results) == self.index \
                and self.index > 0:
            self.results = self.list_next()
            self.index = 0
        if not self.results:
            return None

        entry = self.results[self.index]
        self.index += 1

        return self.process_entry(entry)


class ListSwift(ListStore):
    def list_next(self):
        if self.results:
            marker = self.results[-1]['name']
        else:
            marker = ''
        _, ret = self.conn.get_container(
            self.args.container, marker=marker, limit=1000)
        return ret

    def process_entry(self, entry):
        if self.args.check_slo:
            hdrs = self.conn.head_object(self.args.container, entry['name'])
            if 'x-static-large-object' in hdrs:
                # We need to fetch the SLO and compute the ETag from it
                _, json_manifest = self.conn.get_object(
                    self.args.container, entry['name'],
                    query_string='multipart-manifest=get')
                manifest = json.loads(json_manifest)
                etags = [segment['hash'].decode('hex') for segment in manifest]
                dgst = hashlib.md5(''.join(etags))
                entry['hash'] = '%s-%d' % (dgst.hexdigest(), len(manifest))
        return entry


class ListS3(ListStore):
    def __init__(self, conn, args):
        super(ListS3, self).__init__(conn, args)
        sync_account = 'AUTH_%s' % self.args.account.split(':')[0]
        prefix_dgst = hashlib.md5('%s/%s' % (
            sync_account, self.args.container)).hexdigest()
        prefix_hash = hex(long(prefix_dgst, 16) % 16**6)[2:-1]
        self.prefix = '/'.join([
            prefix_hash, sync_account, self.args.container]) + '/'

    def list_next(self):
        if self.results:
            marker = self.results[-1]['Key']
        else:
            marker = ''
        aws_results = self.conn.list_objects(
            Bucket=self.args.bucket, Prefix=self.prefix, Marker=marker)
        return aws_results.get('Contents', [])


def check_object_listings(args):
    swift_conn = swiftclient.client.Connection(
        args.auth_url, args.account, args.key)
    s3_session = boto3.session.Session(args.access_key, args.secret)
    s3_conn = s3_session.client('s3')

    swift_lister = ListSwift(swift_conn, args)
    s3_lister = ListS3(s3_conn, args)
    missing = []

    swift_entry = next(swift_lister, None)
    s3_entry = next(s3_lister, None)

    while True:
        if not swift_entry and not s3_entry:
            break
    
        if swift_entry and s3_entry:
            if swift_entry['name'] == s3_entry['Key'][len(s3_lister.prefix):]:
                if swift_entry['hash'] != s3_entry['ETag'][1:-1]:
                    missing.append((swift_entry['name'], 'ETag mismatch'))
                swift_entry = next(swift_lister, None)
                s3_entry = next(s3_lister, None)
            elif swift_entry['name'] < s3_entry['Key'][len(s3_lister.prefix):]:
                missing.append((swift_entry['name'], 'missing in S3'))
                swift_entry = next(swift_lister, None)
            else:
                missing.append((s3_entry['Key'][len(s3_lister.prefix):],
                                'missing in Swift'))
                s3_entry = next(s3_lister, None)
        elif swift_entry and s3_entry is None:
            missing.append((swift_entry['name'], 'missing in S3'))
            swift_entry = next(swift_lister, None)
        elif s3_entry and swift_entry is None:
            missing.append((s3_entry['Key'][len(s3_lister.prefix):],
                            'missing in Swift'))
            s3_entry = next(s3_lister, None)
    return missing


def __main__():

    parser = argparse.ArgumentParser(
        description='Check if a Swift container has been synced to AWS')
    
    parser.add_argument('--auth-url', type=str, help='Swift auth URL')
    parser.add_argument('--account', type=str, help='Swift account')
    parser.add_argument('--container', type=str, help='Swift container')
    parser.add_argument('--key', type=str, help='Swift account password')
    parser.add_argument('--bucket', type=str, help='AWS bucket')
    parser.add_argument('--access-key', type=str, help='AWS Access Key ID')
    parser.add_argument('--secret', type=str, help='AWS Secret Access Key')
    parser.add_argument('--check-slo', dest='check_slo', action='store_true',
                        help='Check SLO ETag. Enable if you have SLOs')
    parser.add_argument('--no-check-slo', dest='check_slo',
                        action='store_false',
                        help='Do not check if an object is an SLO')
    parser.set_defaults(feature=False)
    
    missing = check_object_listings(parser.parse_args())
    if missing:
        print 'Missing elements found: %s' % ('\t\n'.join(
            ['%s: %s' % (k, v) for k, v in missing]))
    else:
        print 'All objects match!'


if __name__ == '__main__':
    __main__()
