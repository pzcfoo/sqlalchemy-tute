from orm.models import User, Address, Base
from engine import engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import aliased
from sqlalchemy import func

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


"""
+-------------------------------------------------------------------------+
| 3. Demonstrating relationship user to many addresses
+-------------------------------------------------------------------------+
"""

jack = User(name='jack', full_name='Jack Bean')
session.add(jack)

jack.addresses = [
    Address(email_address='jb@gmail.com', user_id=jack.id),
    Address(email_address='yolo@jb.com', user_id=jack.id),
    Address(email_address='123@123.com', user_id=jack.id)
]

print(jack.addresses)
assert jack.addresses[0].user == jack
session.commit()  # addresses also get comitted


"""
+-------------------------------------------------------------------------+
| 4. Changing a relationship owner
+-------------------------------------------------------------------------+
"""
tyrion = session.query(User).filter_by(name='tyrion').one()

jack.addresses[1].user = tyrion

print(tyrion.addresses)
print(jack.addresses)  # jack no longer had the address[1]
session.commit()

"""
+-------------------------------------------------------------------------+
| 5. Implicit Join
+-------------------------------------------------------------------------+
"""
results = session.query(User, Address).filter(User.id == Address.user_id).all()
print(results)

"""
+-------------------------------------------------------------------------+
| 6. Explicit Join
+-------------------------------------------------------------------------+
"""
results = session.query(User, Address).join(Address, User.id == Address.user_id).all()
print(results)

"""
+-------------------------------------------------------------------------+
| 7. Succinct Join (y)
+-------------------------------------------------------------------------+
"""
results = session.query(User, Address).join(User.addresses).all()
print(results)

# Note, you could also just access User.addresses
"""
+-------------------------------------------------------------------------+
| 7. Simple Join (if foreign keys to join on are not ambiguous)
+-------------------------------------------------------------------------+
"""
results = session.query(User, Address).join(Address).all()
print(results)

"""
+-------------------------------------------------------------------------+
| 8. Aliasing - a query that refers to the same entity more than once
| in the FROM clause requires *aliasing*
+-------------------------------------------------------------------------+
"""

a1, a2 = aliased(Address), aliased(Address)

results = session.query(
                        User
                    ).join(
                        a1
                    ).join(
                        a2
                    ).filter(
                        a1.email_address == 'jb@gmail.com'
                    ).filter(
                        a2.email_address == 'yolo@jb.com'
                    ).all()
print(results)

"""
+-------------------------------------------------------------------------+
| 9. Aliasing sub queries
| The subquery acts as a derived table which you can join on.
+-------------------------------------------------------------------------+
"""
sub_q = session.query(
    User.id.label('user_id'),
    func.count(Address.id).label('count')
).join(
    User.addresses
).group_by(
    User.id
).subquery()

results = session.query(
    User.name,
    func.coalesce(sub_q.c.count, 0),
).outerjoin(
    sub_q,
    User.id == sub_q.c.user_id
).all()

print(results)

"""
+-------------------------------------------------------------------------+
| 10. Lazy Loading - A select statement is emitted for each parent object
| accessed that has a relationship to a child object.
+-------------------------------------------------------------------------+
"""
for user in session.query(User):
    print(user, user.addresses)
pass

"""
+-------------------------------------------------------------------------+
| 11. Eager Loading - solves the 'N plus one' problem where many select
| statements are emitted upon loading collections against a parent result.
|
| Subquery Load - loading all collections at once
+-------------------------------------------------------------------------+
"""
from sqlalchemy.orm import subqueryload
for user in session.query(User).options(subqueryload(User.addresses)):
    print(user, user.addresses)
pass

"""
+-------------------------------------------------------------------------+
| 12. Joined Load - users LEFT OUTER JOIN to load parent + child in on query
+-------------------------------------------------------------------------+
"""
from sqlalchemy.orm import joinedload
for user in session.query(User).options(joinedload(User.addresses)):
    print(user, user.addresses)
pass


"""
+-------------------------------------------------------------------------+
| 13. Join and a subquery load - use contains_eager
+-------------------------------------------------------------------------+
"""
from sqlalchemy.orm import contains_eager
for address in session.query(Address).join(Address.user).options(joinedload(Address.user)):
    print(address, address.user)
pass

# we can go like this instead
for address in session.query(Address).join(Address.user).options(contains_eager(Address.user)):
    print(address, address.user)
pass


"""
+-------------------------------------------------------------------------+
| 14. Cascading deletes on children
+-------------------------------------------------------------------------+
"""
jack = session.query(User).filter_by(name='jack').one()
del jack.addresses[0]  # jacks old address now has a foreign key user_id as None
session.commit()
pass

# configure the relationship on the class to delete-orphan

User.addresses.property.cascade = 'all, delete, delete-orphan'

tyrion = session.query(User).filter_by(name='tyrion').one()
del tyrion.addresses[0]
session.commit()

# -------------------------------------------------------------------------+
# Clean Up
# -------------------------------------------------------------------------+
session.query(Address).delete()
session.query(User).delete()
session.commit()


