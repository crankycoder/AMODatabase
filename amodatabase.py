#!/bin/sh python3

"""
This script requires Python 3
"""

import datetime
from contextlib import contextmanager
import json
import logging
import logging.config
import os
import shutil
import typing
import sys
from pprint import pprint

import requests
from requests_toolbelt.threaded import pool

MAX_PROCESSES = 50
DEFAULT_AMO_REQUEST_URI = "https://addons.mozilla.org/api/v3/addons/search/"
QUERY_PARAMS = "?app=firefox&sort=created&type=extension"
logger = logging.getLogger('amo_database')


@contextmanager
def tinydb(fpath='/tmp/amo_cache'):
    class Cache:
        def __init__(self, fpath):
            self._fpath = fpath
            shutil.rmtree(self._fpath, ignore_errors=True)
            os.mkdir(self._fpath)

        def put(self, data):
            guid = data['guid']
            fname = os.path.join(self._fpath, "%s.json" % guid)
            with open(fname, 'w') as fout:
                print ("Handled: %s" % guid)
                fout.write(json.dumps(data))

    yield Cache(fpath)


class JSONSchema:
    pass


class AMOAddonFile(JSONSchema):
    meta = {
            'id': int,
            'platform': str,
            'status': str,
            'is_webextension': bool
    }


class AMOAddonVersion(JSONSchema):
    meta = {
            'files': typing.List[AMOAddonFile]
    }


class AMOAddonInfo(JSONSchema):
    meta = {
            'guid': str,
            'categories': typing.Dict[str, typing.List[str]],
            'default_locale': str,
            'description': typing.Dict[str, str],
            'name': typing.Dict[str, str],
            'current_version': AMOAddonVersion,
            'ratings': typing.Dict[str, float],
            'summary': typing.Dict[str, str],
            'tags': typing.List[str],
            'weekly_downloads': int
    }


class AMODatabase:
    def __init__(self):
        """
        Just setup the page_count
        """

        uri = DEFAULT_AMO_REQUEST_URI + QUERY_PARAMS
        response = requests.get(uri)
        jdata = json.loads(response.content.decode('utf8'))

        self._page_count = jdata['page_count']

    def fetch_pages(self):
        with tinydb() as db:
            urls = []
            for i in range(1, self._page_count+1):
                url = "%s%s%s" % (DEFAULT_AMO_REQUEST_URI, QUERY_PARAMS, ("&page=%d" % i))
                urls.append(url)
            p = pool.Pool.from_urls(urls, num_processes=MAX_PROCESSES)
            p.join_all()

            self._handle_responses(p, db)

            # Try failed requests
            exceptions = p.exceptions()
            p = pool.Pool.from_exceptions(exceptions, num_processes=MAX_PROCESSES)
            p.join_all()
            self._handle_responses(p, db)

    def _handle_responses(self, p, db):
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']
                    for record in results:
                        db.put(record)
            except Exception as e:
                # Skip this record
                logger.error(e)


class Undefined:
    pass


def is_container(type_def):
    """
    str and bytes are containers in python, but well - that's not
    terribly useful
    """
    if issubclass(type_def, typing.Container) and type_def not in [str, bytes]:
        return True
    return False


def fix_types(value, attr_name, type_def):
    serializers = {
        str: str,
        typing.Dict: dict,
        int: int,
        float: float,
        typing.List: list,
        bool: bool,
        }

    if issubclass(type_def, JSONSchema):
        return marshal(type_def, value)
    elif is_container(type_def):
        if issubclass(type_def, typing.List):
            item_type = type_def.__args__[0]
            return [fix_types(j, attr_name, item_type) for j in value]
        elif issubclass(type_def, typing.Dict):
            if value is None:
                return None
            k_cast, v_cast = type_def.__args__
            dict_vals = [(fix_types(k, attr_name, k_cast),
                          fix_types(v, attr_name, v_cast))
                         for k, v in value.items()]
            return dict(dict_vals)
    else:
        # This is a simple type
        return serializers[type_def](value)


def marshal(schema, json_data):
    """
    Coerce some JSON data using a schema
    """
    obj = {}
    for attr_name, type_def in schema.meta.items():
        value = json_data.get(attr_name, Undefined)
        if value is not Undefined:
            # Try marshalling the value
            obj[attr_name] = fix_types(value, attr_name, type_def)

    return obj

def parse_file(fname):
    jbdata = open('/tmp/amo_cache/%s' % fname, 'rb').read()
    data = json.loads(jbdata.decode('utf8'))
    result = marshal(AMOAddonInfo, data)
    pprint(result)

def main():
    print (datetime.datetime.now())
    for i, fname in enumerate(os.listdir("/tmp/amo_cache/")):
        parse_file(fname)
        if i % 200 == 0:
            sys.stdout.write('.')
            sys.stdout.flush()
    print (datetime.datetime.now())

if __name__ == '__main__':
    # amodb = AMODatabase()
    # amodb.fetch_pages()
    #main()
    parse_file('{feb799e2-29e2-4e35-b862-cc4e1842b6f5}.json')
