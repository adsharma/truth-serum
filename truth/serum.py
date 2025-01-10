import asyncio
import csv
import io
import re
from dataclasses import dataclass, field
from datetime import date
from typing import List

from fquery.sqlmodel import GLOBAL_ID_SEQ, SQL_PK, model
from langchain_ollama import OllamaLLM
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

llm = OllamaLLM(model="qwen2.5:latest")


@model(global_id=True)
@dataclass
class ObjectType:
    name: str
    id: int | None = None


@model("countries", global_id=True)
@dataclass
class Country:
    name: str
    id: int | None = None


@model("cities", global_id=True)
@dataclass
class City:
    name: str
    id: int | None = None


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


engine = create_engine("duckdb:///kg.db", echo=True)
SQLModel.metadata.create_all(engine)


def extract_code(text):
    pattern = r"```(\w+)\n(.*?)```"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        return match.group(2).strip()
    else:
        return None


@task
async def fetch_countries() -> List[Country]:
    logger = get_run_logger()
    # csv is fewer tokens than json
    result = llm.generate(
        [
            """Be short and complete. List 5 countries in the world in English and their capitals as csv.
            No headers, no explanation. One capital only"""
        ]
    )
    text = result.generations[0][0].text
    logger.info(text)
    if text.startswith("```"):
        csv_string = extract_code(text)
    else:
        csv_string = text
    data = [
        {"id": i + 1, "name": row[0], "capital": row[1]}
        for i, row in enumerate(csv.reader(io.StringIO(csv_string)))
        if len(row) == 2
    ]
    countries = [Country(c["name"]) for c in data]
    capitals = [City(c["capital"]) for c in data]
    Session = sessionmaker(bind=engine)

    # This needs to be done only once
    CAPITAL_RELATION = 1000000
    INSTANCE_OF = 1000001
    COUNTRY_TYPE = ObjectType("Country").sqlmodel()
    CITY_TYPE = ObjectType("City").sqlmodel()

    with Session() as session:
        session.add(COUNTRY_TYPE)
        session.add(CITY_TYPE)
        session.commit()
        session.refresh(COUNTRY_TYPE)
        session.refresh(CITY_TYPE)

    # 10 = 5 countries + 5 capitals
    with engine.connect() as conn:
        ids = [conn.execute(func.next_value(GLOBAL_ID_SEQ)).scalar() for _ in range(10)]

    with Session() as session:
        for country, capital in zip(countries, capitals):
            country_model = country.sqlmodel()
            capital_model = capital.sqlmodel()
            country_model.id = ids.pop(0)
            capital_model.id = ids.pop(0)
            session.add(country_model)
            session.add(capital_model)
            print(capital_model.id, country_model.id)
            session.add(
                TypeRelation(
                    src=country_model.id, etype=INSTANCE_OF, dst=COUNTRY_TYPE.id
                ).sqlmodel()
            )
            session.add(
                TypeRelation(
                    src=capital_model.id, etype=INSTANCE_OF, dst=CITY_TYPE.id
                ).sqlmodel()
            )
            relation = Relation(
                src=country_model.id, etype=CAPITAL_RELATION, dst=capital_model.id
            )
            session.add(relation.sqlmodel())
        session.commit()
    return countries


@task
async def process_countries(countries: List[Country]) -> List[str]:
    # Process the countries, e.g., extract names
    return [country.name for country in countries]


@flow
async def async_flow():
    countries = await fetch_countries()
    return await process_countries(countries)


async def async_main():
    # Run the flow
    result = await async_flow()
    print(result)


asyncio.run(async_main())
