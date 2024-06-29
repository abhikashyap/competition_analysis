
from sqlalchemy import create_engine
from db_constants import db_constants
import pandas as pd
from piTask import general
from sqlalchemy.orm import sessionmaker
from datetime import datetime , timedelta
import os
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, MetaData


import logging
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from piTask import database
from sqlalchemy import text
import pandas as pd


now = datetime.now()
day_month_year = now.strftime("%d%m%y")
if "/" in os.getcwd():
    address = os.getcwd() + "/inventory_files"
else:
    address = os.getcwd() + "\inventory_files"

current_dir = os.getcwd()

# Setup basic logging
logging.basicConfig(level=logging.INFO)

download_directory=address

def rename_file(file_name):

    files = os.listdir(download_directory) 
    latest_file=files[-1]
    # Get the latest file based on modification time

    # Construct paths for old and new files
    old_path = os.path.join(download_directory, latest_file)
    print(old_path)
    new_filename = f'{file_name}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    new_path = os.path.join(download_directory, new_filename)

    # Rename the file
    os.rename(old_path, new_path)
    a=sorted(os.listdir(address),reverse=True)


"""
if_exists_mode:


'fail': This is the default value. If the table exists, an exception will be raised.

'replace': If the table exists, it will be replaced (i.e., dropped and recreated), and the new data will be inserted.

'append': If the table exists, the data will be appended to it.

'truncate': If the table exists, it will be truncated (emptied), and the new data will be inserted.

"""


def send_df_to_database(df, table_name, if_exists_mode='append'):
    # PostgreSQL connection details
    username = db_constants.username_aws
    password = db_constants.password_aws
    host = db_constants.host_aws
    port = db_constants.port_aws
    database_name = "dummy"

    # Construct the connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)

    # Send to the database with specified if_exists mode
    df.to_sql(table_name, con=engine, if_exists=if_exists_mode, index=False, method='multi', chunksize=500, schema='public')

    print(f'The data has been inserted into the table {table_name}.')


"""
The function adds a maturity level to certain types of data 
"""
def make_maturity_of_day_level_data(date,df,matured_days):
  
  for day in range(1,matured_days+1):
    df.loc[(df[date] < datetime.today()-timedelta(days=matured_days)),'maturity'] = matured_days
    df.loc[(df[date] > datetime.today()-timedelta(days=day)) & (df[date] < datetime.today()-timedelta(days=(day-1))),'maturity']=day

  return df


def rename_columns(df):
    df.columns = list(map(lambda x: x.lower(), df.columns.to_list()))
    df.columns = list(map(lambda x: x.replace('–', ' ').replace('-', " ").replace('+', "_and ").replace("'s", "").replace('?', " ").replace('  ', "").replace('(', "").replace(')', "").replace('#', "").replace('25', "twenty_five ").replace('5', "five").replace('₹', "").replace('/', "_").replace('1,000', "one_thousand").replace('.', "_"), df.columns.to_list()))
    df.columns = list(map(lambda x: x.replace(' – ', '').replace('-', " "), df.columns.to_list()))
    df.columns = list(map(lambda x: x.lower(), df.columns.to_list()))
    df.columns = list(map(lambda column: column.split('day')[-1].strip(), df.columns))
    df.columns = list(map(lambda x: '_'.join(x.split(' ')), df.columns.to_list()))

    # Check if '%' is at the end of column name
    df.columns = list(map(lambda x: x.replace('%', 'percent_') if not x.endswith('percent_') else x, df.columns))

    # Replace 'percent_' with 'percent' when it's at the end of the column name
    df.columns = list(map(lambda x: x[:-8] + 'percent' if x.endswith('percent_') else x, df.columns))
    
    # Remove consecutive underscores
    df.columns = list(map(lambda x: '_'.join(filter(None, x.split('_'))), df.columns))

    return df


"""
This function changes date to date datatype i
"""

def change_date_column_to_date_data_type_2(df):
    # Iterate through the columns and check for 'date' in column names
    for column in df.columns:
        if 'date' in column.lower():  # Check if 'date' is in the column name (case insensitive)
            try:
                df[column] = pd.to_datetime(df[column], format='%B %d, %Y')
            except ValueError:
                # Try another format with abbreviated month name
                df[column] = pd.to_datetime(df[column], format='%b %d, %Y')
            print(column)
    return df


def change_date_column_to_date_data_type(df):
    # Iterate through the columns and check for 'date' in column names
    for column in df.columns:
        if 'date' in column.lower():  # Check if 'date' is in the column name (case insensitive)
            df[column] = pd.to_datetime(df[column], errors='coerce')  # Convert the column to datetime data type
            print(column)
    return df




def convert_to_float(df):

  # units that were sometimes used as int and sometimes used as range in database

  need_to_check_dtype=['spend', 'gmv', 'amount', 'Fees', 'price', 'mrp', 'revenue', 'ctr', 'cvr', 'cpc', 'acos','tacos',
                       'rate', 'roi', 'count', 'quantity', 'clicks', 'impression', 'view', 'order', 
                       'unit', 'total_sales', 'balance', 'billing_period', 'total','qty','stock','charge','plan']
  for check in need_to_check_dtype:
    for column in df.columns:
      if ((check in column.lower()) & ('date' not in column.lower())):
        # if (df[column].dtype == 'object')  :
        if (df[column].dtype == 'object'):
          print(column)
          df[column] = df[column].astype("str").str.replace(',', '').str.replace('Rs.', '').str.replace('%', '')
          try:
            df[column]=df[column].astype('float').astype('int')
          except:
             try:
                df[column]=df[column].astype('float')
             except:
                print(f'{column} is not converted to float  ')
  return df

def change_to_object(df):
  need_to_check_dtype=['id','code','hsn', 'size','length','weight','breadth','height','color','isbn','query']
  for check in need_to_check_dtype:
    for column in df.columns:
      if ((check in column.lower()) & ('date' not in column.lower())):
        if (df[column].dtype == 'float') | (df[column].dtype == 'int') | (df[column].dtype == 'object') :
          print(column)
          try:
            df[column]=df[column].astype('str').where(df[column].notna())
          except:
             pass
  return df


# def send_updated_data(df, table_model):
#     username = 'blooprint'
#     password = 'blooprint'
#     host = '192.168.1.19'
#     port = '5433'
#     database_name = 'blooprint' 
#     connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

#     engine = create_engine(connection_string)
#     Session = sessionmaker(bind=engine)
#     session = Session()

#     batch_size = 10000  # Adjust based on performance and resource usage

#     try:
#         for start in range(0, len(df), batch_size):
#             end = start + batch_size
#             batch = df.iloc[start:end].where(pd.notnull, None).to_dict(orient='records')
#             session.bulk_insert_mappings(table_model, batch, return_defaults=False)
#             session.commit()
#             logging.info(f"Processed batch {start} to {end}")

#         logging.info("Data sent successfully.")

#     except SQLAlchemyError as e:
#         session.rollback()
#         logging.error("Error occurred:", exc_info=e)

#     finally:
#         session.close()
#.......................................................parallel............................

def append_data_to_table(connection_string, df, table_name):
    engine = create_engine(connection_string)
    with sessionmaker(bind=engine)() as session:
        try:
            data = []
            for index, row in df.iterrows():
                row = row.where(pd.notnull(row), None)
                row_dict = row.to_dict()
                data.append(row_dict)
            
            session.bulk_insert_mappings(table_name, data,render_nulls=True)
            session.commit()
            print("Insert processed successfully")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error occurred: {e}")
            raise
        finally:
            session.close()

def merge_data_to_table(connection_string,df, table_name):
    engine = create_engine(connection_string)
    with sessionmaker(bind=engine)() as session:
        try:
            data =[]
            for index, row in df.iterrows():
                row = row.where(pd.notnull(row), None)
                row_dict = row.to_dict()
                data.append(row_dict)
            session.bulk_update_mappings(table_name,data)
            session.commit()
            print("Insert processed successfully")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error occurred: {e}")

        finally:
            session.close()


def fill_na_in_columns(df):
    for column in df.columns:
        # if df[column].dtype == 'object':
        #     df[column]=df[column].fillna('0').str.replace('nan',"0")
        
        if df[column].dtype == 'float64' or df[column].dtype == 'int64':
            df[column].fillna(0,inplace=True)

        # elif df[column].dtype == 'datetime64[ns]':
        #     print('hi')
        #     df[column].fillna('01/01/2000',inplace=True)
          
        # else:
        #    df[column].fillna(0,inplace=True)
    
    return df



def delete_meta_data_of_table(table_name, connection_string):

    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)
    # Reflect the existing database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name in metadata.tables:
        table_to_drop = metadata.tables[table_name]
        table_to_drop.drop(bind=engine)

        print(f"{table_name} table has been dropped from the metadata.")
    else:
        print(f"{table_name} table not found in the metadata.")
      

def network_database_url():
   # creating session
    username = db_constants.username
    password = db_constants.password
    host = db_constants.host
    port = db_constants.port
    database_name = db_constants.database_name
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'

    return connection_string

def network_database_url_for_academy():
    # PostgreSQL connection details
    username = db_constants.username_bpa
    password = db_constants.password_bpa
    host = db_constants.host_bpa
    port = db_constants.port_bpa
    database_name = db_constants.database_name_bpa

    # Construct the connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'
    return connection_string


def network_database_url_aws():
   # creating session
    username = db_constants.username_aws
    password = db_constants.password_aws
    host = db_constants.host_aws
    port = db_constants.port_aws
    database_name = db_constants.database_name_aws
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'
    return connection_string


def network_database_url_fk_aws():
   # creating session
    username = db_constants.username_aws
    password = db_constants.password_aws
    host = db_constants.host_aws
    port = db_constants.port_aws
    database_name = db_constants.database_name3_aws
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database_name}'
    return connection_string


def convert_strptime_date_format(date_string):
    try:
        return datetime.strptime(date_string, '%b %d %Y').strftime('%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(date_string, '%B %d %Y').strftime('%Y-%m-%d')
        except ValueError:
            return None
        

def remove_commas(value):
    if isinstance(value, str):
        return value.replace(',', '')
    else:
        return value
  

def converter(df, column):
    def convert_value(value):
        if value.strip() == '':
            return 0
        elif 'Lakhs' in value:
            numeric_part = value.replace('Lakhs', '')
            return float(numeric_part) * 100000
        elif 'Crores' in value:
            numeric_part = value.replace('Crores', '')
            return float(numeric_part) * 10000000
        else:
            return float(value)

    df[column] = df[column].apply(convert_value)


def sql_to_df(db_url,file_address):
    db_url = db_url

    engine = create_engine(db_url)

    with open(file_address, 'r') as file:
        sql_query = file.read()

    with engine.connect() as connection:
        result = connection.execute(text(sql_query))
        
        # Fetch the results
        rows = result.fetchall()
        
        df=pd.DataFrame(rows)
    return df


def query_to_df(db_url,sql_query):
    db_url = db_url

    engine = create_engine(db_url)
    with engine.connect() as connection:
        result = connection.execute(text(sql_query))
        
        # Fetch the results
        rows = result.fetchall()
        
        df=pd.DataFrame(rows)
    return df


def query_to_pull_every_thing(Table_name):
    query=f"""select 
        *
    from 
        {Table_name}"""
    return query

def query_to_pull_last_updated_date(Table_name):
    query=f'''WITH ranked_metrics AS (
        SELECT 
            *,
            RANK() OVER (PARTITION BY account_name ORDER BY updated_date DESC) AS rank
        FROM 
            {Table_name}
    )
    SELECT 
        *
    FROM 
        ranked_metrics
    WHERE 
        rank = 1;

    '''
    return query

def pull_latest_data_from_db(table_name,database_url):
    query=f"""WITH ranked_metrics AS (
          SELECT 
              *,
              RANK() OVER (PARTITION BY account_name ORDER BY updated_date DESC) AS rank
          FROM 
              {table_name}
      )
      SELECT 
          *

      FROM 
          ranked_metrics
      WHERE 
          rank = 1
      LIMIT 1;
      """
    df_temp=query_to_df(database_url,query)
    column_names=df_temp.columns.to_list()
    if 'rank' in column_names:
        column_names.remove('rank')
        column_names.remove('updated_date')

        (',').join(column_names)
        final_query=query=f"""WITH ranked_metrics AS (
            SELECT 
                *,
                RANK() OVER (PARTITION BY account_name ORDER BY updated_date DESC) AS rank
            FROM 
                {table_name}
        )
        SELECT 
            {(',').join(column_names)}

        FROM 
            ranked_metrics
        WHERE 
            rank = 1;
        """
        df=query_to_df(database_url,final_query)
        return df




def delete_all_data(account_name):
    db_urls=[database.network_database_url_aws(),database.network_database_url_fk_aws()]
    for db_url in db_urls:
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Get all table names
        table_names = metadata.tables.keys()
        all_table_names=[]
        for table_name in table_names:
            all_table_names.append(table_name)

        
        def delete_data(table_name,account_name):
            formatted_table_name = f'{table_name}' 
            formatted_account_name = f"'{account_name}'" 

            sql_query = f"""
            DELETE FROM {formatted_table_name}
            WHERE account_name = {formatted_account_name}
            """
            with engine.connect() as connection:
                result = connection.execute(text(sql_query))

                num_rows_affected = result.rowcount
                print(f"{num_rows_affected} rows deleted in {formatted_table_name} table.")
                connection.commit()
        for table_name in all_table_names:
            try:
                delete_data(table_name,account_name)
            except:
                pass

def delete_all_data_from_fk(account_name):
    db_urls=[database.network_database_url_fk_aws()]
    for db_url in db_urls:
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Get all table names
        table_names = metadata.tables.keys()
        all_table_names=[]
        for table_name in table_names:
            all_table_names.append(table_name)

        
        def delete_data(table_name,account_name):
            formatted_table_name = f'{table_name}' 
            formatted_account_name = f"'{account_name}'" 

            sql_query = f"""
            DELETE FROM {formatted_table_name}
            WHERE account_name = {formatted_account_name}
            """
            with engine.connect() as connection:
                result = connection.execute(text(sql_query))

                num_rows_affected = result.rowcount
                print(f"{num_rows_affected} rows deleted in {formatted_table_name} table.")
                connection.commit()
        for table_name in all_table_names:
            try:
                delete_data(table_name,account_name)
            except:
                pass

def delete_all_data_from_az(account_name):
    db_urls=[database.network_database_url_aws()]
    for db_url in db_urls:
        engine = create_engine(db_url)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Get all table names
        table_names = metadata.tables.keys()
        all_table_names=[]
        for table_name in table_names:
            all_table_names.append(table_name)

        
        def delete_data(table_name,account_name):
            formatted_table_name = f'{table_name}' 
            formatted_account_name = f"'{account_name}'" 

            sql_query = f"""
            DELETE FROM {formatted_table_name}
            WHERE account_name = {formatted_account_name}
            """
            with engine.connect() as connection:
                result = connection.execute(text(sql_query))

                num_rows_affected = result.rowcount
                print(f"{num_rows_affected} rows deleted in {formatted_table_name} table.")
                connection.commit()
        for table_name in all_table_names:
            try:
                delete_data(table_name,account_name)
            except:
                pass