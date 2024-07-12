import os
import pytest
import polars as pl
from utils.PolarsFileSystem import PolarsFileSystem

@pytest.fixture
def polars_filesystem(tmpdir):
    path = str(tmpdir)
    return PolarsFileSystem(path)

def test_check_schema(polars_filesystem):
    df = pl.DataFrame({
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c']
    })
    store = 'test_store'
    polars_filesystem.create_schema_dict_in_store(df.schema, store)
    assert polars_filesystem.check_schema(df, store) is None

def test_get_schema(polars_filesystem):
    store = 'test_store'
    schema = {
        'A': 'int64',
        'B': 'str'
    }
    polars_filesystem.create_schema_dict_in_store(schema, store)
    assert polars_filesystem.get_schema(store) == schema

def test_create_schema_dict_in_store(polars_filesystem):
    schema = {
        'A': 'int64',
        'B': 'str'
    }
    store = 'test_store'
    polars_filesystem.create_schema_dict_in_store(schema, store)
    assert polars_filesystem.get_schema(store) == schema

def test_get_all_stores(polars_filesystem):
    store1 = 'store1'
    store2 = 'store2'
    polars_filesystem.ensure_folder_exists(os.path.join(polars_filesystem.path, store1))
    polars_filesystem.ensure_folder_exists(os.path.join(polars_filesystem.path, store2))
    assert polars_filesystem.get_all_stores() == [store1, store2]

def test_get_all_tables(polars_filesystem):
    store = 'test_store'
    table1 = 'table1'
    table2 = 'table2'
    polars_filesystem.ensure_folder_exists(os.path.join(polars_filesystem.path, store, table1))
    polars_filesystem.ensure_folder_exists(os.path.join(polars_filesystem.path, store, table2))
    assert polars_filesystem.get_all_tables(store) == [table1, table2]

def test_read_entire_store(polars_filesystem):
    store = 'test_store'
    table1 = 'table1'
    table2 = 'table2'
    data1 = pl.DataFrame({
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c']
    })
    data2 = pl.DataFrame({
        'A': [4, 5, 6],
        'B': ['d', 'e', 'f']
    })
    polars_filesystem.write_dataframe(data1, store, table1, 'A', 'table_df')
    polars_filesystem.write_dataframe(data2, store, table2, 'A', 'table_df')
    result = polars_filesystem.read_entire_store(store)
    expected = pl.concat([data1, data2])
    assert result.frame_equal(expected)

def test_read_dataframe(polars_filesystem):
    store = 'test_store'
    table = 'table'
    index_name = 'A'
    data = pl.DataFrame({
        'A': [1, 2, 3],
        'B': ['a', 'b', 'c']
    })
    polars_filesystem.write_dataframe(data, store, table, index_name, 'table_df')
    result = polars_filesystem.read_dataframe(store, table, index_name)
    assert result.frame_equal(data)