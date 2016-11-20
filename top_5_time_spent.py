import os

# sqlalchemy Imports
from sqlalchemy import (
    create_engine, MetaData, Table, select
)
from sqlalchemy.sql import func, desc
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import join

# Graphing Imports
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
user_accounts = account.columns.id.label('users')
total_seconds = func.sum(time.columns.seconds).label('seconds')
joined_on = join(
    account, time,
    account.columns.id == time.columns.account_id
)
stmt = select([user_accounts, total_seconds]) \
    .select_from(joined_on) \
    .group_by(user_accounts) \
    .order_by(desc(total_seconds))
results = connection.execute(stmt).fetchall()

# Create Pandas DataFrame from results.
index_value = list(range(1, len(results) + 1))
columns = results[0].keys()

df = pd.DataFrame(
    data=results,
    index=index_value,
    columns=columns
)

# Convert seconds to hours.
df['seconds'] = df['seconds'] / 60 / 60  # Convert seconds to hours.

# Rename seconds columns to hours
df.rename(columns={'seconds': 'hours'}, inplace=True)

# Get users and seconds vlaues.
users = df['users'].values
hours = df['hours'].values

# Create x & y ticks from values.
user_xticks = ['user_id: {user}'.format(user=user) for user in users]
hour_yticks = ['hours: {hours}'.format(hours=int(hours)) for hours in hours]

# Create bar graph.
plt.bar(df['users'].values, df['hours'].values, align='center', alpha=0.5)

# Set informative info.
plt.title('Top 5 users who spent the most time on the web.')
plt.xlabel('Unique user id.')
plt.ylabel('Hours spent on the web')

# Set Dynamic y & x ticks.
plt.yticks(hours, hour_yticks)
plt.xticks(users, user_xticks)

# Show bar graph.
plt.show()
