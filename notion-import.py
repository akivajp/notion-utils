#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import subprocess
import yaml

import pandas as pd

from notion_client import Client
from logzero import logger

def get_schema(client: Client, database_id: str):
    res = client.databases.retrieve(
        **{
            "database_id": database_id,
        }
    )
    return res['properties']

def map_row(
    row: dict|pd.Series,
    map_config: dict|None,
) -> dict:
    new_row = {}
    for column, value in row.items():
        if pd.isna(value):
            continue
        if map_config is None:
            new_row[column] = value
        else:
            def get_map_item():
                mapping = map_config.get('map', {})
                if column in mapping:
                    return mapping[column]
                #stripped_column = re.sub(r'\s+', '', column)
                stripped_column = column
                stripped_column = re.sub(r' ',  '',  stripped_column) # スペースの除去
                stripped_column = re.sub(r'\n', ' ', stripped_column) # 改行のスペース化
                if stripped_column in mapping:
                    return mapping[stripped_column]
                logger.debug('column: "%s"', column)
                return {}
            #logger.debug('column: "%s"', column)
            #map_item = map_config.get(column, {})
            #map_item = map_config.get(column.strip(), {})
            map_item = get_map_item()
            db_column = map_item.get('column', None)
            #db_type = map_item.get('type', None)
            #if db_column and db_type:
            new_row[db_column] = value
    logger.debug('map_config: %s', map_config)
    if map_config:
        assign_table = map_config.get('assign', {})
        #logger.debug('assign_table: %s', assign_table)
        for db_column, value in assign_table.items():
            new_row[db_column] = value
    return new_row

def get_filter(
    schema: dict,
    row: dict,
    map_config: dict|None = None,
):
    filters = []
    for column, value in row.items():
        if map_config:
            primary = map_config.get('primary', {})
            if column not in primary:
                continue
        db_type = schema.get(column, {}).get('type', None)
        if db_type == 'title':
            filters.append({
                "property": column,
                "title": {
                    "equals": str(value),
                },
            })
        elif db_type == 'number':
            filters.append({
                "property": column,
                "number": {
                    "equals": float(value),
                },
            })
        elif db_type == 'select':
            filters.append({
                "property": column,
                "select": {
                    "equals": str(value),
                },
            })
    return {"and": filters}

def filter_db(
    client: Client,
    database_id: str,
    filter: dict,
) -> dict:
    res = client.databases.query(
        **{
            "database_id": database_id,
            "filter": filter,
        }
    )
    #logger.debug('res: %s', json.dumps(res, indent=2, ensure_ascii=False))
    return res['results']

def get_properties(
    schema: dict,
    row: dict,
):
    properties = {}
    for column, value in row.items():
        db_type = schema.get(column, {}).get('type', None)
        if db_type == 'title':
            properties[column] = {
                "title": [
                    {
                        "text": {
                            "content": str(value),
                        },
                    },
                ],
            }
        elif db_type == 'number':
            properties[column] = {
                "number": float(value),
            }
        elif db_type == 'rich_text':
            properties[column] = {
                "rich_text": [
                    {
                        "text": {
                            "content": str(value),
                        },
                    },
                ],
            }
        elif db_type == 'select':
            properties[column] = {
                "select": {
                    "name": str(value),
                },
            }
    return properties

def import_df(
    client: Client,
    database_id: str,
    df: pd.DataFrame,
    map_config: dict|None = None,
    before: int|None = None,
    after: int|None = None,
):
    schema = get_schema(client, database_id)
    #logger.debug('schema: %s', json.dumps(schema, indent=2, ensure_ascii=False))
    #return
    for index, row in df.iterrows():
        #if index > 0:
        #if index > 10:
        #    break
        #if test and index >= test:
        #    break
        if before and index >= before:
            break
        if after and index < after:
            continue
        #properties = {}
        mapped_row = map_row(row, map_config)
        logger.debug('mapped_row: %s', mapped_row)
        #for column, value in row.items():
        filter = get_filter(schema, mapped_row, map_config)
        #logger.debug('filter: %s', filter)
        found = filter_db(client, database_id, filter)
        #logger.debug('found: %s', json.dumps(found, indent=2, ensure_ascii=False))
        properties = get_properties(schema, mapped_row)
        title = [p for p in properties if schema[p]['type'] == 'title']
        if not title:
            logger.warning('title not found')
            continue
        logger.debug('properties: %s', properties)
        if found:
            for item in found:
                client.pages.update(
                    **{
                        "page_id": item['id'],
                        "properties": properties,
                    }
                )
        else:
            client.pages.create(
                **{
                    "parent": {
                        "database_id": database_id,
                    },
                    "properties": properties,
                }
            )
        #logger.debug('properties: %s', properties)
        #break

def main():
    parser = argparse.ArgumentParser(description='Import to Notion database')
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
        '--file', '-F',
        required=True,
        help='File path to import',
    )
    parser.add_argument(
        '--map', '-M',
        help='Map file path',
    )
    parser.add_argument(
        '--before', '--test',
        type=int,
        help='before count',
    )
    parser.add_argument(
        '--after',
        type=int,
        help='after count',
    )
    args = parser.parse_args()
    logger.debug('args: %s', args)
    token = args.token
    if not token:
        token = os.getenv('NOTION_TOKEN')
    if not token:
        raise ValueError('token not found')
    client = Client(auth=token)
    map_config = None
    if args.map:
        ext = os.path.splitext(args.map)[1]
        if ext == '.yaml':
            with open(args.map, 'r') as f:
                map_config = yaml.safe_load(f)
                logger.debug('map_config: %s', map_config)
        else:
            raise ValueError(f'unsupported map file extension: {ext}')
    ext = os.path.splitext(args.file)[1]
    if ext == '.xlsx':
        df = pd.read_excel(args.file)
        logger.debug('df: \n%s', df)
        import_df(
            client,
            args.database_id,
            df,
            map_config=map_config,
            before=args.before,
            after=args.after,
        )
    else:
        raise ValueError(f'unsupported file extension: {ext}')

if __name__ == "__main__":
    main()
