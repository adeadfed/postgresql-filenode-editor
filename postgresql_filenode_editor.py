#!/usr/bin/env python3
import argparse
from loguru import logger

from filenode import Filenode
from helpers.argparse_types import Path, Base64Data, CsvData, DataTypeCsv
from helpers.logger import configure_logger


parser = argparse.ArgumentParser()
parser.add_argument(
    '-f',
    '--filenode-path',
    required=True,
    type=Path,
    help='Path to the target PostgreSQL filenode'
)
parser.add_argument(
    '-m',
    '--mode',
    choices=['list', 'read', 'update', 'raw_update'],
    required=True,
    help='List items in the target filenode'
)
parser.add_argument(
    '-p',
    '--page',
    type=int,
    help='Index of the page to read/write'
)
parser.add_argument(
    '-i',
    '--item',
    type=int,
    help='Index of the item to read/write'
)
parser.add_argument(
    '-b',
    '--b64-data',
    type=Base64Data,
    help='New item data to set; encoded in Base64; only available in \
        raw_update mode'
)
parser.add_argument(
    '-c',
    '--csv-data',
    type=CsvData,
    help='New item data to set; encoded in CSV; only available in update mode'
)
parser.add_argument(
    '-d',
    '--datatype-csv',
    type=DataTypeCsv,
    help='Datatype CSV extracted from the PostgreSQL server'
)


if __name__ == '__main__':
    configure_logger()

    args = parser.parse_args()
    filenode = Filenode(args.filenode_path, args.datatype_csv)

    if args.mode == 'list':
        if args.page is not None:
            filenode.list_page(args.page)
        else:
            filenode.list_pages()

    if args.mode == 'read':
        if all(x is not None for x in [
            args.page,
            args.item
        ]):
            filenode.read_item(args.page, args.item)
        else:
            logger.error('please provide page and item indexes via --page and \
                         --item arguments')

    if args.mode == 'raw_update':
        if all(x is not None for x in [
            args.page,
            args.item,
            args.b64_data
        ]):
            filenode.update_item(args.page, args.item, args.b64_data)
            filenode.save_to_path(args.filenode_path.with_suffix('.new'))
        else:
            logger.error('please provide page, item indexes, and new item data\
                          via the --page, --item, and --b64-data arguments')

    if args.mode == 'update':
        if all(x is not None for x in [
            args.page,
            args.item,
            args.csv_data,
            args.datatype_csv
        ]):
            filenode.update_item(args.page, args.item, args.csv_data)
            filenode.save_to_path(args.filenode_path.with_suffix('.new'))
        else:
            logger.error('please provide page, item indexes, and new item data\
                          via the --page, --item, --datatype-csv and \
                         --csv-data arguments')
