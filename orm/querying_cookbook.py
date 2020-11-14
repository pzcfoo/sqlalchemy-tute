import datetime as dt
import pandas as pd

from sqlalchemy.orm import Session
from sqlalchemy import func, literal, case, select, union_all, cast, Date, and_

from engine import engine
from orm.models import Base, Recipe, Ingredient, ExecutedRecipe

Base.metadata.create_all(bind=engine)
session = Session(bind=engine)


session.query(Recipe).delete()
session.query(Ingredient).delete()
session.commit()

# region setup data
date = dt.date(year=2020, month=1, day=1)
date_1 = dt.date(year=2020, month=2, day=1)

session.add_all(
    [
        Recipe(recipe_name="Cake", id=1),
        Recipe(recipe_name="Fried Rice", id=2),
        Ingredient(ingredient_name="Milk", id=1),
        Ingredient(ingredient_name="Eggs", id=2),
        Ingredient(ingredient_name="Flour", id=3),
        Ingredient(ingredient_name="Sugar", id=4),
        Ingredient(ingredient_name="Butter", id=5),
        Ingredient(ingredient_name="Baking Soda", id=6),
        Ingredient(ingredient_name="Steak", id=7),
        Ingredient(ingredient_name="Msg", id=8),
        Ingredient(ingredient_name="Salt", id=9),
        ExecutedRecipe(recipe_id=1, ingredient_id=1, date=date, quantity=200),
        ExecutedRecipe(recipe_id=1, ingredient_id=2, date=date, quantity=100),
        ExecutedRecipe(recipe_id=1, ingredient_id=3, date=date, quantity=500),
        ExecutedRecipe(recipe_id=1, ingredient_id=4, date=date, quantity=80),
        ExecutedRecipe(recipe_id=2, ingredient_id=6, date=date_1, quantity=200),
        ExecutedRecipe(recipe_id=2, ingredient_id=7, date=date_1, quantity=100),
        ExecutedRecipe(recipe_id=2, ingredient_id=8, date=date_1, quantity=500),
        ExecutedRecipe(recipe_id=2, ingredient_id=9, date=date_1, quantity=80),
    ]
)
session.commit()
# endregion

# region Cross Join query


def a_cross_join_query():
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
            Ingredient.ingredient_name.label("ingredient_used"),
        )
        .join(Recipe, ExecutedRecipe.recipe)
        .join(Ingredient, ExecutedRecipe.ingredient)
    ).subquery()

    used_ingredients_cross_q = session.query(
        used_ingredients_sq.c.recipe_name,
        used_ingredients_sq.c.date,
        used_ingredients_sq.c.quantity,
        used_ingredients_sq.c.ingredient_used,
        Ingredient.ingredient_name,
    ).join(Ingredient, literal(True))
    # df = pd.DataFrame(used_ingredients_cross_q)  # debug
    return used_ingredients_cross_q


def b_cross_join_case_query():
    """
    With the product of A x B:
    If ingredient_used != ingredient from the cross product then we want to set the quantity to zero.

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
    cross_join_sq = a_cross_join_query().subquery()
    ingredients_match = (
        cross_join_sq.c.ingredient_used == cross_join_sq.c.ingredient_name
    )
    q = session.query(
        cross_join_sq.c.recipe_name,
        cross_join_sq.c.date,
        case([(ingredients_match, cross_join_sq.c.quantity)], else_=0).label(
            "quantity"
        ),
        cross_join_sq.c.ingredient_name,
    )
    df = pd.DataFrame(q)  # debug
    return q


def c_cross_join_group_by_query():
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

    cross_case_join_sq = b_cross_join_case_query().subquery()
    q = session.query(
        cross_case_join_sq.c.recipe_name,
        cross_case_join_sq.c.date,
        cross_case_join_sq.c.ingredient_name,
        func.sum(cross_case_join_sq.c.quantity).label("quantity"),
    ).group_by(
        cross_case_join_sq.c.recipe_name,
        cross_case_join_sq.c.date,
        cross_case_join_sq.c.ingredient_name,
    )
    df = pd.DataFrame(q)  # debug
    return q


def full_cross_join_query():
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
            Ingredient.ingredient_name.label("ingredient_used"),
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
            Ingredient.ingredient_name,
            func.sum(case([(has_ingredient_used, sq.c.quantity)], else_=0)).label(
                "quantity"
            ),
        )
        .join(Ingredient, literal(True))
        .group_by(sq.c.recipe_name, sq.c.date, Ingredient.ingredient_name,)
    )
    df = pd.DataFrame(q)  # debug
    return q


# endregion

# region Cross Join on two variables query


def a_time_data_as_query():
    dates = [
        dt.date(year=2020, month=1, day=1),
        dt.date(year=2020, month=2, day=1),
        dt.date(year=2020, month=3, day=1),
        dt.date(year=2020, month=4, day=1),
    ]
    select_qs = [select([cast(literal(date), Date).label("date")]) for date in dates]

    q = (union_all(*select_qs)).alias("Dates")
    q = session.query(q)
    df = pd.DataFrame(q)  # debug
    return q


def b_cross_join_on_two_variables_query():
    """
    A - we've already cross joined on our first variable, now the for the second (we want an entry for every date)
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

    B
    ┌──────┐
    │ Date │
    ├──────┤
    │ 1/1  │
    │ 1/2  │
    │ 1/3  │
    │ 1/4  │
    └──────┘

    We end up with 9 x 4 = 36 rows
    ┌─────────────┬─────────────────┬──────────┬───────────┬──────┐
    │ recipe_name │ ingredient_name │ quantity │ date_used │ date │
    ├─────────────┼─────────────────┼──────────┼───────────┼──────┤
    │ Cake        │ Milk            │      200 │ 1/1       │ 1/1  │
    │ Cake        │ Egg             │      100 │ 1/1       │ 1/1  │
    │ Cake        │ Flour           │      500 │ 1/1       │ 1/1  │
    │ Cake        │ Sugar           │       80 │ 1/1       │ 1/1  │
    │ ...         │ ...             │      ... │ ...       │ ...  │
    │ Cake        │ Milk            │        0 │ 1/1       │ 1/2  │
    │ Cake        │ Egg             │        0 │ 1/1       │ 1/2  │
    │ Cake        │ Flour           │        0 │ 1/1       │ 1/2  │
    │ Cake        │ Sugar           │        0 │ 1/1       │ 1/2  │
    └─────────────┴─────────────────┴──────────┴───────────┴──────┘
    """
    date_sq = a_time_data_as_query().subquery()
    sq = full_cross_join_query().subquery()
    has_date = sq.c.date == date_sq.c.date

    q = session.query(
        sq.c.recipe_name,
        sq.c.ingredient_name,
        sq.c.date.label("date_used"),
        date_sq.c.date,
        case([(has_date, sq.c.quantity)], else_=0).label("quantity"),
    ).join(date_sq, literal(True))
    df = pd.DataFrame(q)  # debug
    return q


def c_cross_join_group_by_on_second_var():
    """
    ┌─────────────┬─────────────────┬──────────┬──────┐
    │ recipe_name │ ingredient_name │ quantity │ date │
    ├─────────────┼─────────────────┼──────────┼──────┤
    │ Cake        │ Milk            │      200 │ 1/1  │
    │ Cake        │ Egg             │      100 │ 1/1  │
    │ Cake        │ Flour           │      500 │ 1/1  │
    │ Cake        │ Sugar           │       80 │ 1/1  │
    │ ...         │ ...             │      ... │ ...  │
    │ Cake        │ Milk            │        0 │ 1/2  │
    │ Cake        │ Egg             │        0 │ 1/2  │
    │ Cake        │ Flour           │        0 │ 1/2  │
    │ Cake        │ Sugar           │        0 │ 1/2  │
    │ ...         │ ...             │      ... │ ...  │
    │ Cake        │ Milk            │        0 │ 1/3  │
    │ Cake        │ Egg             │        0 │ 1/3  │
    │ Cake        │ Flour           │        0 │ 1/3  │
    │ Cake        │ Sugar           │        0 │ 1/3  │
    └─────────────┴─────────────────┴──────────┴──────┘
    """
    sq = b_cross_join_on_two_variables_query().subquery()
    q = (
        session.query(
            sq.c.recipe_name,
            sq.c.ingredient_name,
            sq.c.date,
            func.sum(sq.c.quantity).label("quantity"),
        )
        .group_by(sq.c.recipe_name, sq.c.ingredient_name, sq.c.date)
        .order_by(sq.c.recipe_name, sq.c.date, sq.c.ingredient_name)
    )
    df = pd.DataFrame(q)  # debug
    return q


def full_cross_join_on_two_variables_query():
    """
    """
    # date subquery
    date_sq = a_time_data_as_query().subquery()
    # we need 1 subquery here to create a column "ingredient used"
    sq = (
        session.query(
            Recipe.recipe_name,
            ExecutedRecipe.date,
            Ingredient.ingredient_name.label("ingredient_used"),
            ExecutedRecipe.quantity,
        )
        .select_from(ExecutedRecipe)
        .join(Ingredient, ExecutedRecipe.ingredient)
        .join(Recipe, ExecutedRecipe.recipe)
    ).subquery()

    has_ingredient_and_date = and_(
        sq.c.date == date_sq.c.date, sq.c.ingredient_used == Ingredient.ingredient_name
    )

    q = (
        session.query(
            sq.c.recipe_name,
            Ingredient.ingredient_name,
            date_sq.c.date,
            func.sum(case([(has_ingredient_and_date, sq.c.quantity)], else_=0)).label(
                "quantity"
            ),
        )
        .select_from(sq)
        .join(date_sq, literal(True))
        .join(Ingredient, literal(True))
        .group_by(sq.c.recipe_name, Ingredient.ingredient_name, date_sq.c.date,)
        .order_by(sq.c.recipe_name, date_sq.c.date, Ingredient.ingredient_name)
    )
    df = pd.DataFrame(q)
    return q


# endregion

df = pd.DataFrame(full_cross_join_on_two_variables_query())
pivot_df = pd.pivot_table(
    df, index=["recipe_name", "ingredient_name"], columns=["date"], values=["quantity"]
).reset_index()
pass
# region cleanup

session.query(Recipe).delete()
session.query(Ingredient).delete()
session.commit()

# endregion
