from orm.models import User, Base
from engine import engine

"""
+-------------------------------------------------------------------------+
| 1. Parent class for your ORM tables. Generally one base in an application.
+-------------------------------------------------------------------------+
"""


"""
+-------------------------------------------------------------------------+
| 2. Declarative base when subclassed will create a 'model'.
| The two key components are the table and mapper attribute.
| Table - columns, constraints schema etc.
| Mapper - mediates the relationships between the db table and the class.
+-------------------------------------------------------------------------+
"""

ed_user = User(name='ed', full_name='ed jones')
print(ed_user)

# create all tables which subclass Base
Base.metadata.create_all(engine)

from sqlalchemy.orm import Session
session = Session(bind=engine)
session.add(ed_user)
print(session.new)

"""
+-------------------------------------------------------------------------+
| 3. Session will *flush* *pending* objects to the db before each query.
+-------------------------------------------------------------------------+
"""
our_user = session.query(User).filter_by(name='ed').first()
print(our_user)
print(session.new)

"""
+-------------------------------------------------------------------------+
| 4. A transaction is opened but not yet commited, ed is flushed to the session.
| But not yet commited to the db.
+-------------------------------------------------------------------------+
"""

"""
+-------------------------------------------------------------------------+
| 5. Session maintains a *unique* object per identity. ed_user is the same
| object as our_user
+-------------------------------------------------------------------------+
"""

print(ed_user is our_user)
ed_user.haha = 'asdf'


"""
+-------------------------------------------------------------------------+
| 6. Add multiple objects, they'll appear in session.new
+-------------------------------------------------------------------------+
"""
session.add_all([
    User(name='wendey', full_name='wendy weathersmith'),
    User(name='mary', full_name='Mary Contrary'),
    User(name='fred', full_name='Fred Flintstone')
])

print(session.new)

"""
+-------------------------------------------------------------------------+
| 7. Modifying objects that are flushed appear in session.dirty
+-------------------------------------------------------------------------+
"""
ed_user.full_name = 'Ed Jones'
print(session.dirty)

"""
+-------------------------------------------------------------------------+
| 8. Finally committing will trigger a flush and add records to the db.
+-------------------------------------------------------------------------+
"""
session.commit()


"""
+-------------------------------------------------------------------------+
| 9. Post commit transaction is finished and the session invalidates all
| data. Accessing orm_objects will automatically start a new transaction
| and reload their attributes from the database.
+-------------------------------------------------------------------------+
"""
print("accessing user object after session is commited.")
print(ed_user.full_name)  # re accessing ed triggers the transaction


"""
+-------------------------------------------------------------------------+
| 10. Any non commited changes can be rolled back. 
+-------------------------------------------------------------------------+
"""

ed_user.name = 'Eduardo'
fake_user = User(name='fake', full_name='Fake Guy')
session.add(fake_user)
session.flush()
print(session.query(User).filter(User.name.in_(['Eduardo', 'fake'])).all())
session.rollback()
print(fake_user in session)
print(ed_user.name)

"""
+-------------------------------------------------------------------------+
| 11. clean up this tute - delete everything
+-------------------------------------------------------------------------+
"""
session.query(User).delete()
session.commit()
