import asyncio
import csv
import io
import re
from typing import List

from database import Database, engine
from fquery.sqlmodel import GLOBAL_ID_SEQ
from kg import Relation, graph
from langchain_ollama import OllamaLLM
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import func
from sqlmodel import SQLModel

LLM = OllamaLLM(model="qwen2.5:latest")


@graph
class Country:
    name: str
    id: int | None = None


@graph
class City:
    name: str
    id: int | None = None


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
    result = LLM.generate(
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

    # This needs to be done only once
    CAPITAL_RELATION = 1000000

    # 10 = 5 countries + 5 capitals
    with engine.connect() as conn:
        ids = [conn.execute(func.next_value(GLOBAL_ID_SEQ)).scalar() for _ in range(10)]

    with Database().db as session:
        for country, capital in zip(countries, capitals):
            country_model = country.sqlmodel()
            capital_model = capital.sqlmodel()
            country_model.id = ids.pop(0)
            capital_model.id = ids.pop(0)
            session.add(country_model)
            session.add(capital_model)
            print(capital_model.id, country_model.id)
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
    SQLModel.metadata.create_all(engine)
    # Run the flow
    result = await async_flow()
    print(result)


asyncio.run(async_main())
