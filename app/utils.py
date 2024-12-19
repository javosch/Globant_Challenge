from sqlalchemy import Table, Integer, String, DateTime, or_
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
import numpy as np
import os

from app.models import metadata
from app.config import engine

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

# Connect metadata with engine, to reflect tables on DataBase
metadata.reflect(bind=engine)

# Message to send when functions end
message_to_return = {
    'status':'',
    'status_code': 102,
    'affected_rows': 0,
    'operation': '',
    'message': '',
    'data': ''
}

def df_to_schema(df: pd.DataFrame, table: Table):
    """
    Align the DataFrame's column types with the SQLAlchemy table schema.

    Args:
        df (pd.DataFrame): DataFrame containing the data to align.
        table (Table): SQLAlchemy Table object.

    Returns:
        pd.DataFrame: Converted DataFrame.
    """
    # Replace null values with None, the engine works with None
    df = df.replace({np.nan: None})

    for column in table.columns:
        if column.name in df.columns:
            if isinstance(column.type, Integer):
                df[column.name] = df[column.name].astype('Int64', errors='ignore')
                df[column.name] = df[column.name].apply(
                    lambda x: int(x) if (
                        isinstance(x, int) or isinstance(x, float) and not np.isnan(x)
                    ) else None
                )
            elif isinstance(column.type, String):
                df[column.name] = df[column.name].astype(str)
            elif isinstance(column.type, DateTime):
                df[column.name] = pd.to_datetime(df[column.name], errors='ignore').dt.tz_localize(None)

    df = df.replace({np.nan: None})

    return df


# Process CSV and upload to DB
def process_csv(file_path: str) -> dict:
    """
    Take a csv and process the data.

    Args:
        file_path (str): Path to the file.

    Returns:
        dict: Result of the operation with affected rows, status and pd.DataFrame.
    """
    # Final message variables
    status = 'failure'
    status_code = 500
    affected_rows = 0
    operation = 'process_csv'
    message = ''

    try:
        # Infer table name from file name
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        
        try:
            table = metadata.tables.get(table_name)
            
            if table == None:
                    catalog_table = metadata.tables.get('catalog_tables')

                    with engine.begin() as conn:
                        catalog_records = conn.execute(
                            catalog_table.select()
                        )
                        conn.close()

                    df_catalog = pd.DataFrame(
                        catalog_records.fetchall(),
                        columns=catalog_records.keys()
                    ).set_index('file_name')

                    map_names_tables = df_catalog.to_dict()['table_name']
                    try:
                        table_name = map_names_tables[table_name]
                        table = metadata.tables.get(table_name)

                    except:
                        message = f'Table {table_name} does not exist or not containing in catalog table'
                        raise KeyError(message)
        
        except Exception as e:            
            message = str(e)
        
        cols = table.columns.keys()

        # Load CSV data
        df = pd.read_csv(
            file_path
            , sep=','
            , names=cols
        )

        df = df_to_schema(df, table)
        
        status = 'success'
        status_code = 200
        affected_rows = df.shape[0]
    
    except Exception as e:
        message = f"Error processing file {file_path}: {str(e)}"
        df = ''
    
    finally:
        os.remove(file_path)

        message_to_return['status'] = status
        message_to_return['status_code'] = status_code
        message_to_return['operation'] = operation
        message_to_return['affected_rows'] = affected_rows
        message_to_return['message'] = message
        message_to_return['data'] = {'dataframe':df, 'table_name':table_name}

        return message_to_return


# Insert records function
def insert_records(table_name: str, df: pd.DataFrame, id_columns: list[str]) -> dict:
    """
    Insert records into the specified table, ensuring no duplicate primary keys.

    Args:
        table_name (str): Name of the table.
        df (pd.DataFrame): DataFrame containing the data to insert.
        id_columns (list[str]): List of columns representing the primary key.

    Returns:
        dict: Result of the operation with affected rows and status.
    """
    # Final message variables
    status = 'failure'
    status_code = 500
    affected_rows = 0
    operation = 'insert'
    message = ''
    data = ''

    try:
        table = metadata.tables.get(table_name)
        if table == None:
            message = f'Table {table_name} does not exist.'
            raise ValueError(message)
        
        df = df_to_schema(df, table)

        with engine.begin() as conn:
            # Check for existing records with matching IDs
            id_filter = [
                (table.c[col] == df[col].iloc[i]) for i in range(len(df))
                for col in id_columns
            ]
            
            # Build query to find existing records
            existing_query = conn.execute(
                table.select().where(or_(*id_filter))
            )
            existing_records = pd.DataFrame(
                existing_query.fetchall(),
                columns=existing_query.keys()
            )

            # Determine non-existing records
            if not existing_records.empty:
                existing_set = set(existing_records[id_columns].itertuples(index=False, name=None))
                incoming_set = set(df[id_columns].itertuples(index=False, name=None))
                non_existing_set = incoming_set - existing_set
                non_existing_df = df[df[id_columns].apply(tuple, axis=1).isin(non_existing_set)]
            else:
                non_existing_df = df

            print(f'Inserting records into table {table_name}')
            # Insert non-existing records
            if not non_existing_df.empty:
                result = conn.execute(
                    table.insert()
                    , non_existing_df.to_dict(orient='records')
                )
                conn.commit()
                conn.close()

                status = 'success'
                status_code = 200
                affected_rows = result.rowcount

            else:
                conn.close()
                message = 'Only duplicates primary keys detected.'
                raise ValueError(message)

    except Exception as e:
        message = str(e)
    
    finally:

        message_to_return['status'] = status
        message_to_return['status_code'] = status_code
        message_to_return['operation'] = operation
        message_to_return['affected_rows'] = affected_rows
        message_to_return['message'] = message
        message_to_return['data'] = data

        return message_to_return    


# Update records function
def update_records(table_name: str, df: pd.DataFrame, id_columns: list[str]) -> dict:
    """
    Update records in the specified table based on the provided DataFrame.

    Args:
        table_name (str): Name of the table.
        df (pd.DataFrame): DataFrame containing the data to update.
        id_columns (list[str]): List of columns representing the primary key.

    Returns:
        dict: Result of the operation with affected rows and status.
    """
    # Final message variables
    status = 'failure'
    status_code = 500
    affected_rows = 0
    operation = 'update'
    message = ''
    data = ''

    try:
        table = metadata.tables.get(table_name)
        if table == None:
            message = f'Table {table_name} does not exist.'
            raise ValueError(message)

        df = df_to_schema(df, table)
        
        with engine.begin() as conn:
            # Fetch existing records from the database
            existing_query = conn.execute(
                table.select().where(
                    *[table.c[col].in_(df[col].tolist()) for col in id_columns]
                )
            )
            existing_records = pd.DataFrame(existing_query.fetchall(), columns=existing_query.keys())
            existing_records = df_to_schema(existing_records, table)
            
            # Set index to ids
            df = df.set_index(id_columns, drop=False)

            # Identify records that have changed
            existing_records_to_compare = existing_records.set_index(id_columns, drop=False)
            index_existing_records_to_compare = existing_records_to_compare.index.tolist()
            df_to_compare = df.loc[index_existing_records_to_compare]
            
            df_to_insert_filter = df.index.difference(index_existing_records_to_compare)
            df_to_insert = df.loc[df_to_insert_filter]
            
            compare = df_to_compare.compare(existing_records_to_compare, result_names=('new_', 'old_'))
            changed_rows = compare.index.values.tolist()
            
            # Insert records if not exists
            if existing_records.empty:
                result = insert_records(table_name, df, id_columns)
                status = result['status']
                status_code = result['status_code']
                operation = result['operation']
                affected_rows = result['affected_rows']
                message = result['message']
                # df = result['data']
                raise

            elif df_to_insert.shape[0] > 0 and len(changed_rows) == 0:
                result = insert_records(table_name, df_to_insert, id_columns)
                status = result['status']
                status_code = result['status_code']
                operation = result['operation']
                affected_rows = result['affected_rows']
                message = result['message']
                # df = result['data']
                raise

            elif df_to_insert.shape[0] > 0 and len(changed_rows) != 0:
                result = insert_records(table_name, df_to_insert, id_columns)
                affected_rows = result['affected_rows']

            elif df_to_insert.shape[0] == 0 and len(changed_rows) == 0:
                status_code = 200
                status = 'success'
                message = 'No changes detected in the provided data.'
                raise
            
            # Prepare the update operations and extract only new data
            changed_records = df.loc[changed_rows]

            for _, row in changed_records.iterrows():
                id_filter = {col: row[col] for col in id_columns}
                update_values = row.drop(labels=id_columns).to_dict()

                # Check for existing records with matching IDs
                id_filter = [
                    table.c[col] == val for col, val in id_filter.items()
                ]

                result = conn.execute(
                    table.update()
                    .where(*id_filter)
                    .values(**update_values)
                )
                
                affected_rows += result.rowcount
            
            
            conn.commit()
            conn.close()

        status = 'success'
        status_code = 200
        affected_rows += affected_rows

    except Exception as e:
        message = str(e)
    
    finally:

        message_to_return['status'] = status
        message_to_return['status_code'] = status_code
        message_to_return['operation'] = operation
        message_to_return['affected_rows'] = affected_rows
        message_to_return['message'] = message
        message_to_return['data'] = data

        return message_to_return


# Delete records function
def delete_records(table_name: str, df: pd.DataFrame, id_columns: list[str]) -> dict:
    """
    Delete records from the specified table based on the provided DataFrame.

    Args:
        table_name (str): Name of the table.
        df (pd.DataFrame): DataFrame containing the IDs of records to delete.
        id_columns (list[str]): List of columns representing the primary key.

    Returns:
        dict: Result of the operation with affected rows and status.
    """
    # Final message variables
    status = 'failure'
    status_code = 500
    affected_rows = 0
    operation = 'delete'
    message = ''
    data = ''

    try:
        table = metadata.tables.get(table_name)
        if table == None:
            message = f'Table {table_name} does not exist.'
            raise ValueError(message)

        df = df_to_schema(df, table)

        with engine.begin() as conn:
            for _, row in df.iterrows():
                id_filter = {col: row[col] for col in id_columns}

                result = conn.execute(
                    table.delete()
                    .where(*[table.c[col] == val for col, val in id_filter.items()])
                )
                affected_rows += result.rowcount
        
            conn.commit()
            conn.close()

        status = 'success'
        status_code = 200
        affected_rows = affected_rows

    except Exception as e:
        message = str(e)
    
    finally:
        message_to_return['status'] = status
        message_to_return['status_code'] = status_code
        message_to_return['operation'] = operation
        message_to_return['affected_rows'] = affected_rows
        message_to_return['message'] = message
        message_to_return['data'] = data

        return message_to_return


# TODO  Crear una función para llamar al procesamiento y luego pasar la función de insertado o actualizado
