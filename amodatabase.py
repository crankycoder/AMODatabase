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
import urllib
import pprint

import requests
from requests_toolbelt.threaded import pool

MAX_PROCESSES = 100
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
            print("Processing AMO urls")
            p = pool.Pool.from_urls(urls, num_processes=MAX_PROCESSES)
            p.join_all()

            self._handle_responses(p, db)

            # Try failed requests
            exceptions = p.exceptions()
            p = pool.Pool.from_exceptions(exceptions, num_processes=MAX_PROCESSES)
            p.join_all()
            self._handle_responses(p, db)

    def fetch_versions(self):
        def version_gen():
            for i, fname in enumerate(os.listdir("/tmp/amo_cache")):
                guid = fname.split(".json")[0]
                url = "https://addons.mozilla.org/api/v3/addons/addon/%s/versions/" % guid
                yield url

        with tinydb(fpath='/tmp/amo_cache_dates') as date_db:
            print("Processing Version urls")
            p = pool.Pool.from_urls(version_gen(), num_processes=MAX_PROCESSES)
            p.join_all()
            last_page_urls = self._handle_version_responses(p)

            # Now fetch the last version of each addon
            print("Processing Last page urls: %d" % len(last_page_urls))
            p = pool.Pool.from_urls(last_page_urls, num_processes=MAX_PROCESSES)
            p.join_all()

            print ("Writing create dates")
            self._handle_last_version_responses(p, date_db)

    def _handle_last_version_responses(self, p, db):
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']

                    guid = resp.url.split("addon/")[1].split("/versions")[0]
                    guid = urllib.parse.unquote(guid)
                    create_date = results[-1]['files'][0]['created']

                    record = {'guid': guid, 'create_date': create_date}
                    db.put(record)
                    print("GUID: %s  Create date: %s" % (guid, create_date))
            except Exception as e:
                # Skip this record
                logger.error(e)

    def _handle_responses(self, p, db):
        guids = []
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']
                    for record in results:
                        guid = record['guid']
                        guids.append(guid)
                        db.put(record)
            except Exception as e:
                # Skip this record
                logger.error(e)
        return guids

    def _handle_version_responses(self, p):
        page_urls = []
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    page_count = jdata['page_count']
                    page_urls.append(resp.url+"?page=%d" % page_count)
            except Exception as e:
                # Skip this record
                logger.error(e)
        return page_urls


class Undefined:
    pass


def fix_types(value, attr_name, type_def):
    serializers = {typing.List: list,
                   typing.Dict: dict,
                   str: str,
                   int: int,
                   float: float,
                   bool: bool}

    if issubclass(type_def, JSONSchema):
        return marshal(type_def, value)
    elif issubclass(type_def, typing.Container) and type_def not in [str, bytes]:
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


def get_version_info(guid):
    jbdata = open('/tmp/amo_cache_dates/%s.json' % guid, 'rb').read()
    data = json.loads(jbdata.decode('utf8'))
    return data['create_date']


def parse_file(guid):
    jbdata = open('/tmp/amo_cache/%s.json' % guid, 'rb').read()
    data = json.loads(jbdata.decode('utf8'))
    result = marshal(AMOAddonInfo, data)
    result['first_create_date'] = get_version_info(guid)
    return result


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
    # main()
    data = parse_file('{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}')
    pprint.pprint(data)

    # amodb = AMODatabase()
    # amodb.fetch_versions()
