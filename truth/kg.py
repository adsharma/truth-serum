from dataclasses import dataclass, field
from datetime import date
from typing import List

from database import Database, allocate_ids
from fquery.sqlmodel import SQL_PK, model
from sqlmodel import SQLModel, select


@model(global_id=True)
@dataclass
class ObjectType:
    name: str
    id: int | None = None


@model(global_id=True)
@dataclass
class PropertyType:
    name: str
    id: int | None = None


class GraphBase:
    def create_object_type_relation(self, derived):
        if not hasattr(derived, "TYPE"):
            self.create_object_type(derived)
        db = Database().db
        with db as session:
            session.add(
                TypeRelation(
                    src=self.id, rtype=InstanceOf.TYPE.id, dst=self.__class__.TYPE.id
                ).sqlmodel()
            )
            session.commit()

    @classmethod
    def create_object_type(cls, derived):
        cls = derived
        # check if ObjectType exists in db
        db = Database().db
        obj_sqlmodel = ObjectType(cls.__name__).sqlmodel()
        ObjectTypeSQLModel = obj_sqlmodel.__class__
        statement = select(ObjectTypeSQLModel).where(
            ObjectTypeSQLModel.name == cls.__name__
        )
        result = db.exec(statement).first()
        if result is None:
            with db as session:
                session.add(obj_sqlmodel)
                session.commit()
                session.refresh(obj_sqlmodel)
        else:
            obj_sqlmodel = result
        cls.TYPE = ObjectType(obj_sqlmodel.name, obj_sqlmodel.id)

    def __post_init__(self):
        self.create_object_type_relation(self.__class__)


class PropertyBase:
    @classmethod
    def create_property_type(cls):
        # check if PropertyType exists in db
        db = Database().db
        prop_sqlmodel = PropertyType(cls.__name__).sqlmodel()
        PropertyTypeSQLModel = prop_sqlmodel.__class__
        statement = select(PropertyTypeSQLModel).where(
            PropertyTypeSQLModel.name == cls.__name__
        )
        result = db.exec(statement).first()
        if result is None:
            with db as session:
                session.add(prop_sqlmodel)
                session.commit()
                session.refresh(prop_sqlmodel)
        else:
            prop_sqlmodel = result
        cls.TYPE = PropertyType(prop_sqlmodel.name, prop_sqlmodel.id)

    def __post_init__(self):
        if not hasattr(self.__class__, "TYPE"):
            self.create_property_type()


# This is logically equivalent to
# type(cls.__name__, (GraphBase,), {**cls.__dict__, **extras})
#
# but without field ordering issues
def inject_base(cls):
    cls.__annotations__["id"] = int | None
    cls.id = field(default=None)
    cls.create_object_type = GraphBase.create_object_type
    cls.create_object_type_relation = GraphBase.create_object_type_relation
    cls.__post_init__ = GraphBase.__post_init__
    return cls


def inject_property_base(cls):
    extras = {"__post_init__": PropertyBase.__post_init__}
    return type(cls.__name__, (PropertyBase,), {**cls.__dict__, **extras})


def graph(cls):
    return model(global_id=True)(dataclass(inject_base(cls)))


def property(cls):
    return dataclass(inject_property_base(cls))


INFINITY_DATE = date.max


@model(global_id=True)
@dataclass
class Relation:
    src: int = field(**SQL_PK)
    rtype: int = field(**SQL_PK)
    dst: int = field(**SQL_PK)
    start: date = field(default_factory=date.today)
    end: date | None = field(default_factory=lambda: INFINITY_DATE)
    probability: float = field(default_factory=lambda: 1.0)
    viewpoint: int | None = 0


@model(global_id=True)
@dataclass
class TypeRelation:
    src: int = field(**SQL_PK)
    rtype: int = field(**SQL_PK)
    dst: int = field(**SQL_PK)


@property
class InstanceOf:
    pass


def save_graph(
    rows: List, left_model: SQLModel, right_model: SQLModel, relation_class: Relation
) -> int:
    ids = allocate_ids(2 * len(rows))

    with Database().db as session:
        for left, right, *_ in rows:
            left_obj = left_model(left).sqlmodel()
            right_obj = right_model(right).sqlmodel()
            left_obj.id = ids.pop(0)
            right_obj.id = ids.pop(0)
            session.add(left_obj)
            session.add(right_obj)
            relation = Relation(
                src=left_obj.id,
                rtype=relation_class.TYPE.id,
                dst=right_obj.id,
            )
            session.add(relation.sqlmodel())
        session.commit()
    return len(rows)


# Unlike save_graph, this saves only objects of a given type. Relations to be added later
def save_objs(rows: List, left_model: SQLModel) -> int:
    ids = allocate_ids(len(rows))

    with Database().db as session:
        for left in rows:
            left_obj = left_model(*left).sqlmodel()
            left_obj.id = ids.pop(0)
            session.add(left_obj)
        session.commit()
    return len(rows)
