import dataclasses
import json
from typing import TYPE_CHECKING

from sqlalchemy import (
    ForeignKey,
    create_engine,
    func,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    joinedload,
    mapped_column,
    query_expression,
    relationship,
    selectinload,
    sessionmaker,
    with_expression,
)
from typing_extensions import TypeIs

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


def is_dataclass_instance(obj) -> "TypeIs[DataclassInstance]":
    return dataclasses.is_dataclass(obj) and not isinstance(obj, type)


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if is_dataclass_instance(o):
            return dataclasses.asdict(o)
        return super().default(o)


engine = create_engine("sqlite:///:memory:", echo=True)
Session = sessionmaker(bind=engine)


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class Team(Base):
    __tablename__ = "team"
    name: Mapped[str]
    headquarters: Mapped[str]
    id: Mapped[int] = mapped_column(default=None, primary_key=True)

    heroes: Mapped[list["Hero"]] = relationship(init=False, repr=False)

    hero_count: Mapped[int | None] = query_expression(repr=False)


class Hero(Base):
    __tablename__ = "hero"
    name: Mapped[str]
    secret_name: Mapped[str]
    age: Mapped[int]
    team_id: Mapped[int] = mapped_column(ForeignKey("team.id"))
    team: Mapped[Team] = relationship(init=False, repr=False)
    id: Mapped[int] = mapped_column(default=None, primary_key=True)


if __name__ == "__main__":
    with Session() as session:
        print("=" * 80)
        Base.metadata.create_all(engine)

    with Session() as session:
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

    with Session() as session:
        print("=" * 80)

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

    with Session() as session:
        print("=" * 80)
        result = session.execute(
            select(
                Hero,
            ).options(
                joinedload(Hero.team),
            )
        ).all()
        for row in result:
            hero, *_ = row._t
            print(hero.name, hero.team.name)

    with Session() as session:
        print("=" * 80)
        result = session.execute(
            select(
                Team,
            ).options(
                selectinload(Team.heroes),
            )
        ).all()
        for row in result:
            team, *_ = row._t
            print(team, team.heroes)

    with Session() as session:
        print("=" * 80)
        result = session.execute(
            select(
                Team,
            )
            .options(
                with_expression(
                    Team.hero_count,
                    func.count(Hero.id),
                )
            )
            .join(Hero, full=True)
            .group_by(Team.id)
        ).all()
        for row in result:
            team, *_ = row._t
            print(team, team.hero_count)
