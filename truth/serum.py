import asyncio
import csv
import io
import re
from typing import List

from database import engine
from kg import InstanceOf, save_graph, save_objs
from langchain_ollama import OllamaLLM
from prefect import flow, task
from prefect.logging import get_run_logger
from schema.events import Event  # noqa F401: need this for the schema to be loaded
from schema.people import Person  # noqa F401: need this for the schema to be loaded
from schema.places import CapitalRelation, City, Country
from schema.things import Building  # noqa F401: need this for the schema to be loaded
from schema.topics import Topic  # noqa F401: need this for the schema to be loaded
from schema.viewpoints import (  # noqa F401: need this for the schema to be loaded
    Viewpoint,
)
from sqlmodel import SQLModel

LLM = OllamaLLM(model="qwen2.5:latest")


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

    rows = list(csv.reader(io.StringIO(csv_string)))
    return save_graph(rows, Country, City, CapitalRelation)


@flow
async def async_flow():
    n = await fetch_countries()
    print(f"saved {n} country, capital, pairs")
    rows = csv.reader(open("truth/seed/viewpoints.csv"))
    next(rows)  # skip header
    n = save_objs(list(rows), Viewpoint)
    print(f"saved {n} viewpoints")
    return n


def init_edge_types():
    CapitalRelation()
    InstanceOf()


async def async_main():
    init_edge_types()
    # Run the flow
    result = await async_flow()
    print(result)


asyncio.run(async_main())
