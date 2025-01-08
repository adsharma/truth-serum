import asyncio
import csv
import io
import re
from datetime import date
from typing import List

from fquery.sqlmodel import model
from langchain_ollama import OllamaLLM
from prefect import flow, task
from prefect.logging import get_run_logger
from pydantic.dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

llm = OllamaLLM(model="qwen2.5:latest")


@model("countries")
@dataclass
class Country:
    id: int
    name: str
    capital: str


@model("relations")
@dataclass
class Relation:
    id: int
    src: int
    type: int
    target: int
    start: date
    end: date
    viewpoint: int


engine = create_engine("sqlite:///kg.db", echo=True)
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
            """Be short and complete. List all countries in the world in English and their capitals as csv.
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
    countries = [Country(**c) for c in data]
    Session = sessionmaker(bind=engine)

    with Session() as session:
        for c in countries:
            session.add(c.sqlmodel())
        session.commit()
    return countries


@task
async def process_countries(countries: List[Country]) -> List[str]:
    # Process the countries, e.g., extract names
    return [country.capital for country in countries]


@flow
async def async_flow():
    countries = await fetch_countries()
    return await process_countries(countries)


async def async_main():
    # Run the flow
    result = await async_flow()
    print(result)


asyncio.run(async_main())
