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


INSTANCE_OF = 1000001


class GraphBase:
    def create_object_type_relation(self):
        if not hasattr(self.__class__, "TYPE"):
            self.create_object_type()
        db = Database().db
        with db as session:
            session.add(
                TypeRelation(
                    src=self.id, etype=INSTANCE_OF, dst=self.__class__.TYPE.id
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


def graph(cls):
    return model(global_id=True)(cls)


INFINITY_DATE = date.max


@model(global_id=True)
@dataclass
class Relation:
    src: int = field(**SQL_PK)
    etype: int = field(**SQL_PK)
    dst: int = field(**SQL_PK)
    start: date = field(default_factory=date.today)
    end: date | None = field(default_factory=lambda: INFINITY_DATE)
    viewpoint: int | None = 0


@model(global_id=True)
@dataclass
class TypeRelation:
    src: int = field(**SQL_PK)
    etype: int = field(**SQL_PK)
    dst: int = field(**SQL_PK)
