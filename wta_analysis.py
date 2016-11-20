import os
from sqlalchemy import (
    create_engine, MetaData, Table, select
)
from sqlalchemy.sql import func
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import join

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
print(time.columns)
print(host.columns)
print(account.columns)
print(host_time.columns)

# Select all accounts.
stmt = select([account])
results = connection.execute(stmt).fetchall()
print(results)

# Select all accounts and times & group by account.
joined_on = join(
    account, time,
    account.columns.id == time.columns.account_id
)
stmt = select([account.columns.id, func.sum(time.columns.seconds)]).select_from(joined_on)
stmt = stmt.group_by(account.columns.id, time.columns.day)
results = connection.execute(stmt).fetchall()
print(results)
