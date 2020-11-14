from engine import engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import select, join


def run_example():
    metadata = MetaData()
    user_table = Table(
        "example_user",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )

    address_table = Table(
        "address",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email_address", String(100), nullable=False),
        Column("user_id", Integer, ForeignKey("example_user.id")),
    )

    network_table = Table(
        "network",
        metadata,
        Column("network_id", Integer, primary_key=True),
        Column("name", String(100), nullable=False),
        Column("created_at", DateTime, nullable=False),
        Column("owner_id", Integer, ForeignKey("example_user.id")),
    )
    metadata.create_all(engine)

    """
    +-------------------------------------------------------------------------+
    | Inserting some data to get us started.
    +-------------------------------------------------------------------------+
    """
    user_data = [
        {"id": 1, "name": "swag", "fullname": "swag king"},
        {"id": 2, "name": "yolo", "fullname": "yolo swag"},
    ]
    stmt_ins_user = user_table.insert().values(user_data)
    engine.execute(stmt_ins_user)

    address_data = [
        {"id": 1, "email_address": "swag.king@gmail.com", "user_id": 1},
        {"id": 2, "email_address": "yolo.swag@gmail.com", "user_id": 2},
    ]
    stmt_ins_address = address_table.insert().values(address_data)
    engine.execute(stmt_ins_address)

    """
    +-------------------------------------------------------------------------+
    | Simple select
    +-------------------------------------------------------------------------+
    """
    stmt_select = select([user_table])
    rc = engine.execute(stmt_select)
    results = rc.fetchall()
    print(results)

    """
    +-------------------------------------------------------------------------+
    | Simple select specifying columns.
    +-------------------------------------------------------------------------+
    """
    stmt_select_specific = select([user_table.c.id, user_table.c.fullname])
    rc = engine.execute(stmt_select_specific)
    results = rc.fetchall()
    print(results)

    """
    +-------------------------------------------------------------------------+
    | Easy joins - uses foreign key constraints on the table objects.
    +-------------------------------------------------------------------------+
    """
    stmt_join = user_table.join(address_table)
    print(stmt_join)

    """
    +-------------------------------------------------------------------------+
    | Explicit join - uses select, select_from and join keywords.
    +-------------------------------------------------------------------------+
    """
    on_expression = user_table.c.id == address_table.c.user_id
    join_expression = join(user_table, address_table, on_expression)
    stmt = select([user_table, address_table]).select_from(join_expression)
    rc = engine.execute(stmt)
    results = rc.fetchall()
    print(results)

    """
    +-------------------------------------------------------------------------+
    | Subquery
    +-------------------------------------------------------------------------+
    """
    # subquery select
    # todo

    # clean up
    metadata.drop_all(engine)


if __name__ == "__main__":
    run_example()
