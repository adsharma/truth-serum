from dataclasses import dataclass, field
from datetime import date

from database import Database
from fquery.sqlmodel import SQL_PK, model
from sqlmodel import select


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
    def create_object_type_relation(self):
        if not hasattr(self.__class__, "TYPE"):
            self.create_object_type()
        db = Database().db
        with db as session:
            session.add(
                TypeRelation(
                    src=self.id, rtype=InstanceOf.TYPE.id, dst=self.__class__.TYPE.id
                ).sqlmodel()
            )
            session.commit()

    @classmethod
    def create_object_type(cls):
        print(f"Initializing {cls.__name__} in db")
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
        self.create_object_type_relation()


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


def inject_base(cls):
    extras = {"__post_init__": GraphBase.__post_init__}
    return type(cls.__name__, (GraphBase,), {**cls.__dict__, **extras})


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
