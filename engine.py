"""
Engine Facts
Executing via engine directly is called connectionless execution.
The engine connects and disconnects for us.

Using a connection is called explicit execution.
We control the span of a connection in use.

Engine usually uses a connection pool.

SQL sent to engine.execute() as a string is not modified, is consumed by the DBAPI verbatim.
"""
import os
from sqlalchemy import create_engine

pg_user = os.getenv("PGUSER", "postgres")
host = os.getenv("PGHOST", "localhost")
pg_pass = os.getenv("PGPASSWORD", "postgres")
db_name = "test"
engine = create_engine(
    f"postgresql://{pg_user}:{pg_pass}@{host}:5432/{db_name}", echo=True
)

"""
+-------------------------------------------------------------------------+
| Executing raw sql using the engine object.
+-------------------------------------------------------------------------+
"""
if __name__ == "__main__":
    engine.execute(
        "CREATE TABLE employee( id int not null, name varchar(40) not null);"
    )
    engine.execute("INSERT INTO employee VALUES (1, 'tyrion')")
    result_cursor = engine.execute("SELECT * FROM employee")
    results = result_cursor.fetchall()
    for r in results:
        print(r)
    result_cursor.close()
    engine.execute("DROP TABLE employee")
