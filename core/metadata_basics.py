"""
Metadata
The basis for SQL Generation and object relational mapping

Popularised by Martin Fowler, Patterns of Enterprise Architecture

Describes structure of database
- tables
- columns
- constraints
In terms of data structures

Generate to a schema.
Generate from a schema.
"""
from engine import engine

from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import inspect


def run_example():
    """
    +-------------------------------------------------------------------------+
    | Creating tables using 'metadata'
    +-------------------------------------------------------------------------+
    """
    # create a table which you can use to generate SQL
    # it is not bound to engine or anything
    metadata = MetaData()
    user_table = Table('example_user', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String),
                       Column('fullname', String)
                       )

    # create a specific table
    address_table = Table('address', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('email_address', String(100), nullable=False),
                          Column('user_id', Integer, ForeignKey('example_user.id'))
                          )

    # it will skip tables which already exist
    metadata.create_all(engine)

    network_table = Table('network', metadata,
                          Column('network_id', Integer, primary_key=True),
                          Column('name', String(100), nullable=False),
                          Column('created_at', DateTime, nullable=False),
                          Column('owner_id', Integer, ForeignKey('example_user.id'))
                          )
    # create a single table
    try:
        network_table.create(engine)
    except:
        pass

    """
    +-------------------------------------------------------------------------+
    | Reflecting a table that we created.
    +-------------------------------------------------------------------------+
    """
    metadata_reflected = MetaData()
    user_reflected = Table('example_user', metadata, autoload=True, autoload_with=engine)

    """
    +-------------------------------------------------------------------------+
    | Inspecting tables using the inspector.
    +-------------------------------------------------------------------------+
    """
    inspector = inspect(engine)
    print(inspector.get_table_names())
    print(inspector.get_columns('example_user'))


if __name__ == '__main__':
    run_example()
