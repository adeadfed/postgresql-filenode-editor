import csv
import pathlib
from io import StringIO
from base64 import b64encode
from filenode import Filenode
from pg_types import DataType

FILENODE_PATH = './tests/sample_filenodes'

sample_filenodes = [
    {
        'name': '1260',
        'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;oid,oid,4,i;rolname,name,64,c;rolsuper,bool,1,c;rolinherit,bool,1,c;rolcreaterole,bool,1,c;rolcreatedb,bool,1,c;rolcanlogin,bool,1,c;rolreplication,bool,1,c;rolbypassrls,bool,1,c;rolconnlimit,int4,4,i;rolpassword,text,-1,i;rolvaliduntil,timestamptz,8,d',
    },
    {
        'name': '40996',
        'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;user_id,int4,4,i;birthday,date,4,i;username,varchar,-1,i;email,varchar,-1,i;password,varchar,-1,i;address,text,-1,i;role,int4,4,i;active,bool,1,c'
    },
    {
        'name': '41014',
        'datatype': 'tableoid,oid,4,i;cmax,cid,4,i;xmax,xid,4,i;cmin,cid,4,i;xmin,xid,4,i;ctid,tid,6,s;id,int4,4,i;name,varchar,-1,i;age,int4,4,i;city,varchar,-1,i',
        'payload_dt_inline': '42,Test,42,Test',
        'payload_dt': '42,Test,42,super loooooooooooooong string',
        'payload_raw': b'\x42\x00\x00\x00\x0bTest\x00\x00\x00\x42\x00\x00\x00\x0bTest'
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
        
        filenode.get_item(page_id=0, item_id=0)

def test_get_datatype():
    for sample_filenode in sample_filenodes:
        filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])

        datatype = DataType(sample_filenode['datatype'])
        filenode = Filenode(filenode_path, datatype=datatype)
        
        filenode.get_item(page_id=0, item_id=0)


def test_update_raw():
    for sample_filenode in sample_filenodes:
        if sample_filenode['name'] == '41014':
            filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
            filenode_new_path = pathlib.Path('/tmp/', sample_filenode['name']).with_suffix('.new')

            filenode = Filenode(filenode_path)
            filenode.update_item(0, 0, b64encode(sample_filenode['payload_raw']))
            filenode.save_to_path(filenode_new_path)

            filenode = Filenode(filenode_new_path)
            assert filenode.get_item(0, 0) == sample_filenode['payload_raw']

def test_update_datatype_inline():
    for sample_filenode in sample_filenodes:
        if sample_filenode['name'] == '41014':
            csv_payload = list(csv.reader(StringIO(sample_filenode['payload_dt_inline'])))[0]

            datatype = DataType(sample_filenode['datatype'])

            filenode_path = pathlib.Path(FILENODE_PATH, sample_filenode['name'])
            filenode_new_path = pathlib.Path('/tmp/', sample_filenode['name']).with_suffix('.new')

            filenode = Filenode(filenode_path, datatype=datatype)
            filenode.update_item(0, 0, csv_payload)
            filenode.save_to_path(filenode_new_path)

            filenode = Filenode(filenode_new_path, datatype=datatype)
            
            updated_values = list()
            for field in filenode.get_item(0, 0):
                value = field['value']
                if isinstance(value, bytes):
                    value = value.decode()
                else:
                    value = str(value)
                updated_values.append(value)
            
            assert sample_filenode['payload_dt_inline'] == ','.join(updated_values)

# def test_update_datatype_new_item():

# def test_update_datatype_null():
            
# def test_update_datatype_new_page():