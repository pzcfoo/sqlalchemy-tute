from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
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


class Address(Base):
    __tablename__ = 'address'

    id = Column(Integer(), primary_key=True, autoincrement=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer(), ForeignKey('example_user.id'))

    user = relationship("User", backref='addresses')

    def __repr__(self):
        return '<Address({})'.format(self.email_address)

