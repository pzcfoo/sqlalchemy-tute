from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy import ForeignKey, UniqueConstraint
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


class Ingredient(Base):
    """
    ┌────┬─────────────────┐
    │ id │ ingredient_name │
    ├────┼─────────────────┤
    │  1 │ salt            │
    │  2 │ sugar           │
    │  3 │ egg             │
    │  4 │ milk            │
    └────┴─────────────────┘
    """
    __tablename__ = 'ingredient'

    id = Column(Integer(), primary_key=True, autoincrement=True)
    ingredient_name = Column(String, nullable=False)


class Recipe(Base):
    """
    ┌────┬─────────────┐
    │ id │ recipe_name │
    ├────┼─────────────┤
    │  1 │ cake        │
    └────┴─────────────┘
    """
    __tablename__ = 'recipe'

    id = Column(Integer(), primary_key=True, autoincrement=True)
    recipe_name = Column(String, nullable=False)


class ExecutedRecipe(Base):
    """
    ┌────┬─────────────┬─────────────────┬──────┬──────────┐
    │ id │ recipe_name │ ingredient_name │ date │ quantity │
    ├────┼─────────────┼─────────────────┼──────┼──────────┤
    │  1 │ cake        │ egg             │ 1/1  │      200 │
    │  2 │ cake        │ sugar           │ 1/1  │       50 │
    │  3 │ cake        │ milk            │ 1/1  │      200 │
    │  4 │ cake        │ flour           │ 1/1  │      500 │
    └────┴─────────────┴─────────────────┴──────┴──────────┘
    """
    __tablename__ = "executedrecipe"

    id = Column(Integer(), primary_key=True, autoincrement=True)
    recipe_id = Column(Integer(), ForeignKey('recipe.id', ondelete='cascade'))
    ingredient_id = Column(Integer(), ForeignKey('ingredient.id', ondelete='cascade'))
    date = Column(Date(), nullable=False)
    quantity = Column(Float(), nullable=False)

    UniqueConstraint('recipe_id', 'ingredient_id', 'date')

    recipe = relationship("Recipe")
    ingredient = relationship("Ingredient")
