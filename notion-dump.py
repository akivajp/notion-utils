#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import subprocess

from notion_client import Client
from logzero import logger

def dump_json(data: dict):
    if subprocess.run('which jq > /dev/null', shell=True).returncode == 0:
        input_data = bytes(json.dumps(data), encoding='utf-8')
        subprocess.run('jq .', input=input_data, shell=True)
    else:
        print(json.dumps(data, indent=2))

def main():
    parser = argparse.ArgumentParser(description='Dump Notion database')
    parser.add_argument(
        '--token', '-T',
        help='Notion token',
    )
    parser.add_argument(
        '--database_id', '-D',
        required=True,
        help='Notion database ID',
    )
    parser.add_argument(
        '--simplify', '-S',
        action='store_true',
        help='Simplify output',
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    token = args.token
    if not token:
        token = os.getenv('NOTION_TOKEN')
    if not token:
        raise ValueError('token not found')
    client = Client(auth=token)
    res = client.databases.query(
        **{
            "database_id": args.database_id,
        }
    )
    if not args.simplify:
        dump_json(res)
    else:
        results = res['results']
        rows = []
        for result in results:
            row = {}
            for key, value in result.items():
                if key == 'properties':
                    properties = value
                    for key, value in properties.items():
                        #row[key] = value['title'][0]['plain_text']
                        if value['type'] == 'title':
                            if value['title']:
                                title = value['title'][0]
                                row[key] = title['plain_text']
                        else:
                            row[key] = value
            rows.append(row)
        dump_json(rows)

if __name__ == "__main__":
    main()
