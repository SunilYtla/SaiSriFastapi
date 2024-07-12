import os
import polars as pl
import glob
from collections import OrderedDict
import json
import pickle

class PolarsFileSystem:
    """
    A class to manage Polars DataFrame operations within a file system, ensuring schema consistency,
    and providing methods to read and write data.

    Attributes:
    -----------
    path : str
        The base directory path where the files are stored.
    """

    def __init__(self, path):
        """
        Initialize the PolarsFileSystem with a base path.

        Parameters:
        -----------
        path : str
            The base directory path where the files are stored.
        """
        self.path = path

    def check_schema(self, df, store):
        """
        Check if the schema of the given DataFrame matches the schema stored in the file system.

        Parameters:
        -----------
        df : polars.DataFrame
            The DataFrame whose schema is to be checked.
        store : str
            The store (directory) name where the schema is saved.

        Raises:
        -------
        Exception:
            If the schema of the DataFrame does not match the stored schema.
        """
        schema = self.get_schema(store)
        if schema is None:
            self.ensure_folder_exists(os.path.join(self.path, store))
            self.create_schema_dict_in_store(df.schema, store)
        else:
            if schema == df.schema:
                pass
            else:
                raise Exception('Schema does not match that of the store')

    def get_schema(self, store):
        """
        Retrieve the schema for a given store from the file system.

        Parameters:
        -----------
        store : str
            The store (directory) name where the schema is saved.

        Returns:
        --------
        OrderedDict or None:
            The schema as an OrderedDict if found, otherwise None.
        """
        schema_path = os.path.join(self.path, store, f"{store}.pkl")

        if not os.path.exists(schema_path):
            return None

        # Load from pickle file
        with open(schema_path, 'rb') as pickle_file:
            loaded_data = pickle.load(pickle_file)

        return loaded_data

    def create_schema_dict_in_store(self, schema, store):
        """
        Create and save the schema for a given store in the file system.

        Parameters:
        -----------
        schema : OrderedDict
            The schema to be saved.
        store : str
            The store (directory) name where the schema will be saved.
        """

        dict_path = os.path.join(self.path, store, f"{store}.pkl")
        
        with open(dict_path, 'wb') as pickle_file:
            pickle.dump(schema, pickle_file)

    def create_json_path_tree(self):
        """
        Create a JSON file called json_path_tree.json to track all the files in the file system.
        This method will modify the JSON whenever new data is created to keep track of it.
        """
        pass

    def write_dataframe(self, polars_dataframe, store_name, table_name, index_name, provided):
        """
        Write a Polars DataFrame to the file system.

        Parameters:
        -----------
        polars_dataframe : polars.DataFrame
            The DataFrame to be written.
        store_name : str
            The store (directory) name where the DataFrame will be saved.
        table_name : str
            The table (file) name where the DataFrame will be saved.
        index_name : str
            The index name for the DataFrame.
        provided : str
            Indicates the type of data being provided. Options are 'index_df', 'table_df', 'full_table'.

        Raises:
        -------
        NotImplementedError:
            If 'full_table' is provided as this feature is not yet implemented.
        """
        self.check_schema(polars_dataframe, store_name)
        if provided == 'index_df':
            final_path = os.path.join(self.path, store_name, table_name)
            self.write_table(polars_dataframe, final_path, index_name)
        elif provided == 'table_df':
            indices = polars_dataframe[index_name].unique().to_list()
            for i in indices:
                final_path = os.path.join(self.path, store_name, table_name)
                self.write_table(polars_dataframe.filter(pl.col(index_name) == i), final_path, i)
        elif provided == 'full_table':
            raise NotImplementedError("Will be implemented in the future. Not sure!")

    def write_table(self, table, path, name, write_json_tree=True):
        """
        Write a Polars DataFrame to a Parquet file.

        Parameters:
        -----------
        table : polars.DataFrame
            The DataFrame to be written.
        path : str
            The directory path where the file will be saved.
        name : str
            The name of the file (without extension).
        write_json_tree : bool, optional
            Whether to update the JSON path tree. Default is True.
        """
        self.ensure_folder_exists(path)
        table.write_parquet(os.path.join(path, f"{name}.parquet"))
        if write_json_tree:
            pass

    def get_all_stores(self):
        """
        Retrieve all store names in the base directory.

        Returns:
        --------
        list:
            A list of store names.
        """
        return self.get_subfolders(self.path)

    def get_all_tables(self, store):
        """
        Retrieve all table names in a given store.

        Parameters:
        -----------
        store : str
            The store (directory) name.

        Returns:
        --------
        list:
            A list of table names.
        """
        parquet_files = self.get_parquet_files(os.path.join(self.path, store))
        tables = [os.path.basename(x).split('.')[0] for x in parquet_files]
        return tables

    def read_entire_store(self, store_name):
        """
        Read all data from a given store.

        Parameters:
        -----------
        store_name : str
            The store (directory) name.

        Returns:
        --------
        polars.DataFrame:
            A DataFrame containing all the data from the store.
        """
        store_path = os.path.join(self.path, store_name)
        parquet_files = glob.glob(os.path.join(store_path, '*/*.parquet'))
        return self.get_df_from_parquet(parquet_files)
    
    def delete_index(self, store_name, table_name, index_name):
        """
        Delete a specific index from a table in a store.

        Parameters:
        -----------
        store_name : str
            The store (directory) name.
        table_name : str
            The table (file) name.
        index_name : str
            The index name to be deleted.
        """
        final_path = os.path.join(self.path, store_name, table_name, f"{index_name}.parquet")
        if os.path.exists(final_path):
            os.remove(final_path)
            print(f"Index deleted: {index_name}")
        else:
            print(f"Index not found: {index_name}")
    
    def check_index_exists(self, store_name, table_name, index_name):
        """
        Check if a specific index exists in a table in a store.

        Parameters:
        -----------
        store_name : str
            The store (directory) name.
        table_name : str
            The table (file) name.
        index_name : str
            The index name to be checked.

        Returns:
        --------
        bool:
            True if the index exists, False otherwise.
        """
        final_path = os.path.join(self.path, store_name, table_name, f"{index_name}.parquet")
        return os.path.exists(final_path)

    def get_df_from_parquet(self, parquet_path_list):
        """
        Read multiple Parquet files into a single DataFrame.

        Parameters:
        -----------
        parquet_path_list : list
            A list of paths to Parquet files.

        Returns:
        --------
        polars.DataFrame:
            A DataFrame containing the combined data from all the Parquet files.
        """
        df_final = None
        for p in parquet_path_list:
            pl_df = pl.read_parquet(p)
            if len(pl_df) > 0:
                if df_final is not None:
                    df_final = pl.concat([df_final, pl_df])
                else:
                    df_final = pl_df
        return df_final

    def read_dataframe(self, store_name, table_name, index_name=None):
        """
        Read a DataFrame from the file system.

        Parameters:
        -----------
        store_name : str
            The store (directory) name.
        table_name : str
            The table (file) name.
        index_name : str, optional
            The index name for the DataFrame. Default is None.

        Returns:
        --------
        polars.DataFrame:
            The DataFrame read from the file system.
        """
        if index_name is None:
            folder_path = os.path.join(self.path, store_name, table_name)
            parquet_files = self.get_parquet_files(folder_path)
            return self.get_df_from_parquet(parquet_files)

        final_path = os.path.join(self.path, store_name, table_name, f"{index_name}.parquet")
        return pl.read_parquet(final_path)

    def get_parquet_files(self, folder_path):
        """
        Retrieve all Parquet files in a given folder.

        Parameters:
        -----------
        folder_path : str
            The directory path.

        Returns:
        --------
        list:
            A list of paths to Parquet files.
        """
        parquet_files = glob.glob(os.path.join(folder_path, '*.parquet'))
        return parquet_files

    def ensure_folder_exists(self, folder_path):
        """
        Ensure that a folder exists; if not, create it.

        Parameters:
        -----------
        folder_path : str
            The directory path to check/create.
        """
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"Folder created: {folder_path}")
        else:
            print(f"Folder already exists: {folder_path}")

    def get_subfolders(self, folder_path, exclude_folders=None):
        """
        Retrieve all subfolders in a given folder, excluding specified ones.

        Parameters:
        -----------
        folder_path : str
            The directory path.
        exclude_folders : list, optional
            A list of folder names to exclude. Default is None.

        Returns:
        --------
        list:
            A list of subfolder names.
        """
        if exclude_folders is None:
            exclude_folders = []

        all_items = os.listdir(folder_path)
        subfolders = [
            item for item in all_items 
            if os.path.isdir(os.path.join(folder_path, item)) and item not in exclude_folders
        ]
        return subfolders

if __name__ == "__main__":
    path = r'C:\Users\sunil\PythonProjects\FastApi\PolarsData'

    pfs = PolarsFileSystem(path)

    store1 = 'demo_store_1'
    table1 = 'demo_table_1'

    data = {
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'Los Angeles', 'Chicago'],
        'Company': ['saisri', 'saisri', 'saisri']
    }

    df = pl.DataFrame(data)

    pfs.write_dataframe(df, store1, table1, 'saisri', provided = 'index_df')

    pfs.read_entire_store(store1)