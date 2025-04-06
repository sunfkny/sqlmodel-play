from contextlib import contextmanager
from typing import Annotated, Any, Literal

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import joinedload as sa_joinedload
from sqlalchemy.orm import query_expression as sa_query_expression
from sqlalchemy.orm import selectinload as sa_selectinload
from sqlalchemy.orm import with_expression as sa_with_expression
from sqlalchemy.sql._typing import _ColumnExpressionArgument
from sqlmodel import (
    Field,
    Relationship,
    Session,
    SQLModel,
    col,
    create_engine,
    func,
    select,
)

engine = create_engine("sqlite:///:memory:", echo=True)


@contextmanager
def get_session():
    with Session(bind=engine) as session:
        yield session


def joinedload[T: SQLModel](
    *keys: Literal["*"] | T | Mapped[T],
):
    return sa_joinedload(*keys)  # type: ignore


def selectinload[T: SQLModel](
    *keys: Literal["*"] | list[T] | Mapped[list[T]],
):
    return sa_selectinload(*keys)  # type: ignore


def with_expression[T](
    key: Literal["*"] | T | Mapped[T],
    expression: _ColumnExpressionArgument[T],
):
    return sa_with_expression(key, expression)  # type: ignore


def QueryExpression() -> Any:
    return sa_query_expression()


class Team(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    headquarters: str

    heroes: list["Hero"] = Relationship()

    hero_count: Annotated[int | None, Field(exclude=True)] = QueryExpression()


class Hero(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: int
    team_id: int = Field(foreign_key="team.id")
    team: Team = Relationship(
        # sa_relationship=RelationshipProperty(lazy="joined"),
    )


if __name__ == "__main__":
    with get_session() as session:
        print("=" * 80)
        SQLModel.metadata.create_all(engine)

    with get_session() as session:
        print("=" * 80)
        session.add_all(
            [
                Team(name="Justice League", headquarters="Metropolis"),
                Team(name="Avengers", headquarters="New York City"),
                Team(name="Guardians of the Galaxy", headquarters="New York City"),
                Team(name="Fantastic Four", headquarters="New York City"),
            ]
        )
        session.commit()

        session.add_all(
            [
                Hero(
                    name="Wonder Woman",
                    secret_name="Diana Prince",
                    age=29,
                    team_id=1,
                ),
                Hero(
                    name="Iron Man",
                    secret_name="Tony Stark",
                    age=48,
                    team_id=2,
                ),
                Hero(
                    name="Star-Lord",
                    secret_name="Peter Quill",
                    age=32,
                    team_id=3,
                ),
                Hero(
                    name="Mr. Fantastic",
                    secret_name="Reed Richards",
                    age=35,
                    team_id=3,
                ),
            ]
        )
        session.commit()

    with get_session() as session:
        print("=" * 80)
        result = session.exec(
            select(
                Hero,
            ).options(
                joinedload(col(Hero.team)),
            )
        ).all()
        for hero in result:
            print(hero.name, hero.team.name)

    with get_session() as session:
        print("=" * 80)
        result = session.exec(
            select(
                Team,
            ).options(
                selectinload(Team.heroes),
            )
        ).all()
        for team in result:
            print(team, team.heroes)

    with get_session() as session:
        print("=" * 80)
        result = session.exec(
            select(
                Team,
            )
            .options(
                with_expression(
                    col(Team.hero_count),
                    func.count(col(Hero.id)),
                )
            )
            .join(Hero, full=True)
            .group_by(col(Team.id))
        ).all()
        for team in result:
            print(team, team.hero_count)
