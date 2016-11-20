import os

# sqlalchemy Imports
from sqlalchemy import (
    create_engine, MetaData, Table, select
)
from sqlalchemy.sql import func, desc
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import join

# Graphing.
import pandas as pd
import matplotlib.pyplot as plt

# Constants
DB_CRED = os.getenv('DB_CRED', '')
DB_PATH = os.getenv('DB_PATH', '')
DB_FULL_PATH = 'postgres://{cred}{path}'.format(cred=DB_CRED, path=DB_PATH)

# Create engine, connection & metadata object.
engine = create_engine(DB_FULL_PATH, echo=True)
connection = Connection(engine).connect()
metadata = MetaData()

# Print tables.
print(engine.table_names())

# Reflect all of the tables.
time = Table('time', metadata, autoload=True, autoload_with=engine)
host = Table('host', metadata, autoload=True, autoload_with=engine)
account = Table('account', metadata, autoload=True, autoload_with=engine)
host_time = Table('host_time', metadata, autoload=True, autoload_with=engine)

# Print each tables columns.
print('Print out each table columns:')
print('time table:      {columns}'.format(columns=time.columns))
print('host table:      {columns}'.format(columns=host.columns))
print('account table:   {columns}'.format(columns=account.columns))
print('host_time table: {columns}'.format(columns=host_time.columns))

# Select all accounts.
print('Select all accounts:')
stmt = select([account.columns.id])
results = connection.execute(stmt).fetchall()
print('Number of accounts: {amnt}'.format(amnt=len(results)))

# Select all accounts and times & group by account.
total_seconds = func.sum(time.columns.seconds).label('total_seconds')
joined_on = join(
    account, time,
    account.columns.id == time.columns.account_id
)
stmt = select([account.columns.id, total_seconds]) \
    .select_from(joined_on) \
    .group_by(account.columns.id) \
    .order_by(desc(total_seconds))
results = connection.execute(stmt).fetchall()
print(results)
