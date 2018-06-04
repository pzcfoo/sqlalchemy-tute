from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class User(Base):
    __tablename__ = 'example_user'  # user clashes with psql user

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String())
    full_name = Column(String())

    def __repr__(self):
        return "<User({}, {}, {})>".format(
            self. id,
            self.name,
            self.full_name
        )
