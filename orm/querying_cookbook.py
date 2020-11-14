import datetime as dt
import pandas as pd

from sqlalchemy.orm import Session
from sqlalchemy.orm import aliased
from sqlalchemy import func, literal, case

from engine import engine
from orm.models import Base, Recipe, Ingredient, ExecutedRecipe

Base.metadata.create_all(bind=engine)
session = Session(bind=engine)


session.query(Recipe).delete()
session.query(Ingredient).delete()
session.commit()

# region setup data
date = dt.date(year=2020, month=1, day=1)

session.add_all([
    Recipe(recipe_name='Cake', id=1),
    Ingredient(ingredient_name='Milk', id=1),
    Ingredient(ingredient_name='Eggs', id=2),
    Ingredient(ingredient_name='Flour', id=3),
    Ingredient(ingredient_name='Sugar', id=4),
    Ingredient(ingredient_name='Butter', id=5),
    Ingredient(ingredient_name='Baking Soda', id=6),
    Ingredient(ingredient_name='Steak', id=7),
    Ingredient(ingredient_name='Msg', id=8),
    Ingredient(ingredient_name='Salt', id=9),
    ExecutedRecipe(recipe_id=1, ingredient_id=1, date=date, quantity=200),
    ExecutedRecipe(recipe_id=1, ingredient_id=2, date=date, quantity=100),
    ExecutedRecipe(recipe_id=1, ingredient_id=3, date=date, quantity=500),
    ExecutedRecipe(recipe_id=1, ingredient_id=4, date=date, quantity=80),
])
session.commit()
# endregion

# region query


def a_cartesian_join_query():
    """
    A
    ┌────┬─────────────┬─────────────────┬──────┬──────────┐
    │ id │ recipe_name │ ingredient_name │ date │ quantity │
    ├────┼─────────────┼─────────────────┼──────┼──────────┤
    │  1 │ Cake        │ Egg             │ 1/1  │      200 │
    │  2 │ Cake        │ Sugar           │ 1/1  │       50 │
    │  3 │ Cake        │ Milk            │ 1/1  │      200 │
    │  4 │ Cake        │ Flour           │ 1/1  │      500 │
    └────┴─────────────┴─────────────────┴──────┴──────────┘

    B
    ┌────┬─────────────────┐
    │ id │ ingredient_name │
    ├────┼─────────────────┤
    │  1 │ Milk            │
    │  2 │ Eggs            │
    │  3 │ Flour           │
    │  4 │ Sugar           │
    │  5 │ Butter          │
    │  6 │ Baking Soda     │
    │  7 │ Steak           │
    │  8 │ Msg             │
    │  9 │ Salt            │
    └────┴─────────────────┘

    A x B
    4 * 9 = 36 rows
    ┌────────┬──────┬──────────┬─────────────────┬────────────┐
    │ recipe │ date │ quantity │ ingredient_used │ ingredient │
    ├────────┼──────┼──────────┼─────────────────┼────────────┤
    │ Cake   │ 1/1  │      200 │ Milk            │ Milk       │
    │ Cake   │ 1/1  │      100 │ Egg             │ Milk       │
    │ Cake   │ 1/1  │       80 │ Sugar           │ Milk       │
    │ Cake   │ 1/1  │      500 │ Flour           │ Milk       │
    │ ...    │ ...  │      ... │ ...             │ ...        │
    │ Cake   │ 1/1  │      200 │ Milk            │ Steak      │
    │ Cake   │ 1/1  │      100 │ Egg             │ Steak      │
    │ Cake   │ 1/1  │       80 │ Sugar           │ Steak      │
    │ Cake   │ 1/1  │      500 │ Flour           │ Steak      │
    └────────┴──────┴──────────┴─────────────────┴────────────┘
    """
    used_ingredients_sq = (
        session.query(
            ExecutedRecipe.quantity,
            ExecutedRecipe.date,
            Recipe.recipe_name,
            Ingredient.ingredient_name.label('ingredient_used'),
        )
        .join(Recipe, ExecutedRecipe.recipe)
        .join(Ingredient, ExecutedRecipe.ingredient)

    ).subquery()

    used_ingredients_cross_q = (
        session.query(
            used_ingredients_sq.c.recipe_name,
            used_ingredients_sq.c.date,
            used_ingredients_sq.c.quantity,
            used_ingredients_sq.c.ingredient_used,
            Ingredient.ingredient_name,
        )
        .join(Ingredient, literal(True))
    )
    # df = pd.DataFrame(used_ingredients_cross_q)  # debug
    return used_ingredients_cross_q


def b_cartesian_join_case_query():
    """
    With the product of A x B:
    If ingredient_used != ingredient from the cartesian product then we want to set the quantity to zero.

    ┌────────┬──────┬──────────┬─────────────────┬────────────┐
    │ recipe │ date │ quantity │ ingredient_used │ ingredient │
    ├────────┼──────┼──────────┼─────────────────┼────────────┤
    │ Cake   │ 1/1  │      200 │ Milk            │ Milk       │
    │ Cake   │ 1/1  │        0 │ Egg             │ Milk       │
    │ Cake   │ 1/1  │        0 │ Sugar           │ Milk       │
    │ Cake   │ 1/1  │        0 │ Flour           │ Milk       │
    │ ...    │ ...  │      ... │ ...             │ ...        │
    │ Cake   │ 1/1  │        0 │ Milk            │ Steak      │
    │ Cake   │ 1/1  │        0 │ Egg             │ Steak      │
    │ Cake   │ 1/1  │        0 │ Sugar           │ Steak      │
    │ Cake   │ 1/1  │        0 │ Flour           │ Steak      │
    └────────┴──────┴──────────┴─────────────────┴────────────┘

    Ingredient_used column serves no purpose after the case statement is used.
    ┌────────┬──────┬──────────┬────────────┐
    │ recipe │ date │ quantity │ ingredient │
    ├────────┼──────┼──────────┼────────────┤
    │ Cake   │ 1/1  │      200 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ ...    │ ...  │      ... │ ...        │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    └────────┴──────┴──────────┴────────────┘
    """
    cartesian_join_sq = a_cartesian_join_query().subquery()
    ingredients_match = cartesian_join_sq.c.ingredient_used == cartesian_join_sq.c.ingredient_name
    q = (
        session.query(
            cartesian_join_sq.c.recipe_name,
            cartesian_join_sq.c.date,
            case([(ingredients_match, cartesian_join_sq.c.quantity)], else_=0).label('quantity_used'),
            cartesian_join_sq.c.ingredient_name,
        )
    )
    # df = pd.DataFrame(q)  # debug
    return q


def c_cartesian_join_group_by_query():
    """
    We group by previous query Table C which has 36 rows:
    ┌────────┬──────┬──────────┬────────────┐
    │ recipe │ date │ quantity │ ingredient │
    ├────────┼──────┼──────────┼────────────┤
    │ Cake   │ 1/1  │      200 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ Cake   │ 1/1  │        0 │ Milk       │
    │ ...    │ ...  │      ... │ ...        │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Steak      │
    └────────┴──────┴──────────┴────────────┘

    D result in one ingredient per entry which results in 9 entries
    ┌────────┬──────┬──────────┬────────────┐
    │ recipe │ date │ quantity │ ingredient │
    ├────────┼──────┼──────────┼────────────┤
    │ Cake   │ 1/1  │      200 │ Milk       │
    │ Cake   │ 1/1  │      500 │ Flour      │
    │ Cake   │ 1/1  │      100 │ Eggs       │
    │ Cake   │ 1/1  │       80 │ Sugar      │
    │ ...    │ ...  │      ... │ ...        │
    │ Cake   │ 1/1  │        0 │ Steak      │
    │ Cake   │ 1/1  │        0 │ Msg        │
    └────────┴──────┴──────────┴────────────┘
    """

    cartesian_case_join_sq = b_cartesian_join_case_query().subquery()
    q = (
        session.query(
            cartesian_case_join_sq.c.recipe_name,
            cartesian_case_join_sq.c.date,
            cartesian_case_join_sq.c.ingredient_name,
            func.sum(cartesian_case_join_sq.c.quantity_used).label('quantity_used'),
        )
        .group_by(
            cartesian_case_join_sq.c.recipe_name,
            cartesian_case_join_sq.c.date,
            cartesian_case_join_sq.c.ingredient_name,
        )
    )
    # df = pd.DataFrame(q)  # debug
    return q


def full_cartesian_join_query():
    """
    ┌────────┬──────┬──────────┬─────────────┐
    │ recipe │ date │ quantity │ ingredient  │
    ├────────┼──────┼──────────┼─────────────┤
    │ Cake   │ 1/1  │      200 │ Milk        │
    │ Cake   │ 1/1  │      500 │ Flour       │
    │ Cake   │ 1/1  │      100 │ Eggs        │
    │ Cake   │ 1/1  │       80 │ Sugar       │
    │ Cake   │ 1/1  │        0 │ Butter      │
    │ Cake   │ 1/1  │        0 │ Baking Soda │
    │ Cake   │ 1/1  │        0 │ Steak       │
    │ Cake   │ 1/1  │        0 │ Msg         │
    └────────┴──────┴──────────┴─────────────┘
    """

   # we need 1 subquery here to create a column "ingredient used"
    sq = (
        session.query(
            Recipe.recipe_name,
            ExecutedRecipe.date,
            Ingredient.ingredient_name.label('ingredient_used'),
            ExecutedRecipe.quantity,
        )
        .select_from(ExecutedRecipe)
        .join(Ingredient, ExecutedRecipe.ingredient)
        .join(Recipe, ExecutedRecipe.recipe)
    ).subquery()

    has_ingredient_used = sq.c.ingredient_used == Ingredient.ingredient_name

    q = (
        session.query(
            sq.c.recipe_name,
            sq.c.date,
            # case([(has_ingredient_used, func.sum(sq.c.quantity))], else_=0).label('quantity'),
            func.sum(case([(has_ingredient_used, sq.c.quantity)], else_=0)).label('quantity'),
            Ingredient.ingredient_name,
        )
        .join(Ingredient, literal(True))
        .group_by(
            sq.c.recipe_name,
            sq.c.date,
            Ingredient.ingredient_name,
            # sq.c.ingredient_used,
        )
    )
    # df = pd.DataFrame(q)  # debug
    return q

# endregion

full_cartesian_join_query()

# region cleanup

session.query(Recipe).delete()
session.query(Ingredient).delete()
session.commit()

# endregion