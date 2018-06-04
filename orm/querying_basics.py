from orm.models import User, Base
from engine import engine
from sqlalchemy.orm import Session

Base.metadata.create_all(bind=engine)
session = Session(bind=engine)

"""
+-------------------------------------------------------------------------+
| 0. Adding some dummy data
+-------------------------------------------------------------------------+
"""
session.add_all([
    User(name='ed', full_name='Ed Jones'),
    User(name='tyrion', full_name='Tyrion Lannister'),
    User(name='jon', full_name='Jon Snow')
])
session.commit()

"""
+-------------------------------------------------------------------------+
| 1. querying use orm.query using an orm class
+-------------------------------------------------------------------------+
"""
query = session.query(User).filter(User.name == 'tyrion').order_by(User.id)
print(query.all())


"""
+-------------------------------------------------------------------------+
| 2. querying specific columns
+-------------------------------------------------------------------------+
"""
query = session.query(User.full_name, User.id)
print(query.all())


"""
+-------------------------------------------------------------------------+
| 3. Array indexes will OFFSET to that index and limit by one
+-------------------------------------------------------------------------+
"""
result = session.query(User).order_by(User.id)[1]
print(result)

session.query(User).delete()
session.commit()
