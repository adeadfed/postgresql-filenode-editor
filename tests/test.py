import csv
import pathlib
import tempfile
from io import StringIO
from base64 import b64encode
from filenode import Filenode
from pg_types import DataType

FILENODE_PATH = './tests/sample_filenodes'

sample_filenodes = [
    # {
    #     'name': '1260',
    #     'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;oid,oid,4,i;rolname,name,64,c;rolsuper,bool,1,c;rolinherit,bool,1,c;rolcreaterole,bool,1,c;rolcreatedb,bool,1,c;rolcanlogin,bool,1,c;rolreplication,bool,1,c;rolbypassrls,bool,1,c;rolconnlimit,int4,4,i;rolpassword,text,-1,i;rolvaliduntil,timestamptz,8,d',
    # },
    {
        'name': '40996',
        'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;user_id,int4,4,i;birthday,date,4,i;username,varchar,-1,i;email,varchar,-1,i;password,varchar,-1,i;address,text,-1,i;role,int4,4,i;active,bool,1,c',
        'payload_dt_inline': '1,42,Test,Test@test.com,Testpass123,Test Address,42,1',
        'payload_dt_null':'1,NULL,Test,NULL,Testpass123,Test Address,42,NULL',
        'payload_dt_long': '1,42,SuperLongStringAAAAAAAAA,SuperLongStringAAAAAAAAAAAAAAA,SuperLongStringAAAAAAAAAAAAAAA,SuperLongStringAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA,42,1',
        'payload_raw': b'\x01\x00\x00\x00\x42\x42\x42\x42\rTest1!test1@test1.com\x1btest1pass123\x69Suite 491 22979 Veum Grove, Gorczanymouth, NH 12345\x00\x42\x00\x00\x00\x01'
    },
    {
        'name': '41014',
        'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;id,int4,4,i;name,varchar,-1,i;age,int4,4,i;city,varchar,-1,i',
        'payload_dt_inline': '42,Test,43,Test1',
        'payload_dt_null':'42,Test1,43,NULL',
        'payload_dt_long': '42,Test1,43,super loooooooooooooong string',
        'payload_raw': b'\x42\x00\x00\x00\x0cTest1\x00\x00\x00\x43\x00\x00\x00\x0bTest'
    }
]

def test_list_all_raw():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode = Filenode(filenode_path)
        
        filenode.list_pages()

def test_list_all_datatype():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])

        datatype = DataType(sample_filenode['datatype'])
        filenode = Filenode(filenode_path, datatype=datatype)
        
        filenode.list_pages()

def test_list_one_raw():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode = Filenode(filenode_path)
        
        filenode.list_page(page_id=0)

def test_list_one_datatype():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])

        datatype = DataType(sample_filenode['datatype'])
        filenode = Filenode(filenode_path, datatype=datatype)
        
        filenode.list_page(page_id=0)


def test_get_raw():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode = Filenode(filenode_path)
        
        filenode.read_item(page_id=0, item_id=0)

def test_get_datatype():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])

        datatype = DataType(sample_filenode['datatype'])
        filenode = Filenode(filenode_path, datatype=datatype)
        
        filenode.read_item(page_id=0, item_id=0)


def test_update_raw():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode_new_path = pathlib.Path(
            tempfile.gettempdir(), 
            sample_filenode['name']
        ).with_suffix('.new')

        filenode = Filenode(filenode_path)
        filenode.update_item(0, 0, b64encode(sample_filenode['payload_raw']))
        filenode.save_to_path(filenode_new_path)

        filenode = Filenode(filenode_new_path)
        assert filenode.read_item(0, 0) == sample_filenode['payload_raw']

def test_update_datatype_inline():
    for sample_filenode in sample_filenodes:
        csv_payload = list(csv.reader(StringIO(sample_filenode['payload_dt_inline'])))[0]

        datatype = DataType(sample_filenode['datatype'])

        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode_new_path = pathlib.Path(
            tempfile.gettempdir(), 
            sample_filenode['name']
        ).with_suffix('.new')

        filenode = Filenode(filenode_path, datatype=datatype)
        filenode.update_item(0, 0, csv_payload)
        filenode.save_to_path(filenode_new_path)

        filenode = Filenode(filenode_new_path, datatype=datatype)
        
        updated_values = list()
        for field in filenode.read_item(0, 0):
            value = field['value']
            if isinstance(value, bytes):
                value = value.decode()
            else:
                value = str(value)
            updated_values.append(value)
        
        assert sample_filenode['payload_dt_inline'] == ','.join(updated_values)

def test_update_datatype_null():
    for sample_filenode in sample_filenodes:
        csv_payload = list(csv.reader(StringIO(sample_filenode['payload_dt_null'])))[0]

        datatype = DataType(sample_filenode['datatype'])

        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode_new_path = pathlib.Path(
            tempfile.gettempdir(), 
            sample_filenode['name']
        ).with_suffix('.new')

        filenode = Filenode(filenode_path, datatype=datatype)
        filenode.update_item(0, 0, csv_payload)
        filenode.save_to_path(filenode_new_path)

        filenode = Filenode(filenode_new_path, datatype=datatype)
        
        print(filenode.read_item(0, 0))

        updated_values = list()
        for field in filenode.read_item(0, 0):
            print(field)
            value = field['value']
            if field['is_null']:
                value = 'NULL'
            elif isinstance(value, bytes):
                value = value.decode()
            else:
                value = str(value)
            updated_values.append(value)
        
        assert sample_filenode['payload_dt_null'] == ','.join(updated_values)

def test_update_datatype_new_item():
    for sample_filenode in sample_filenodes:
        csv_payload = list(csv.reader(StringIO(sample_filenode['payload_dt_long'])))[0]

        datatype = DataType(sample_filenode['datatype'])

        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
        filenode_new_path = pathlib.Path(
            tempfile.gettempdir(), 
            sample_filenode['name']
        ).with_suffix('.new')

        filenode = Filenode(filenode_path, datatype=datatype)
        # update item in last page
        last_page = len(filenode.pages) - 1

        filenode.update_item(last_page, 0, csv_payload)
        filenode.save_to_path(filenode_new_path)

        filenode = Filenode(filenode_new_path, datatype=datatype)
        # read item from the last page
        last_page = len(filenode.pages) - 1
        
        updated_values = list()
        # new item be the last in the page
        for field in filenode.read_item(last_page, len(filenode.pages[last_page].items) - 1):
            value = field['value']
            if isinstance(value, bytes):
                value = value.decode()
            else:
                value = str(value)
            updated_values.append(value)
        
        assert sample_filenode['payload_dt_long'] == ','.join(updated_values)

def test_update_datatype_new_page():
    # test 40996 only
    sample_filenode = sample_filenodes[0]

    csv_payload = list(csv.reader(StringIO(sample_filenode['payload_dt_long'])))[0]

    datatype = DataType(sample_filenode['datatype'])

    filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
    filenode_new_path = pathlib.Path(
        tempfile.gettempdir(), 
        sample_filenode['name']
    ).with_suffix('.new')

    filenode = Filenode(filenode_path, datatype=datatype)
    
    # update item in first page
    filenode.update_item(0, 0, csv_payload)
    filenode.save_to_path(filenode_new_path)

    filenode = Filenode(filenode_new_path, datatype=datatype)
    
    # read item from the last page
    last_page = len(filenode.pages) - 1
    
    updated_values = list()
    # new item be the last in the page
    for field in filenode.read_item(last_page, len(filenode.pages[last_page].items) - 1):
        value = field['value']
        if isinstance(value, bytes):
            value = value.decode()
        else:
            value = str(value)
        updated_values.append(value)
    
    assert sample_filenode['payload_dt_long'] == ','.join(updated_values)