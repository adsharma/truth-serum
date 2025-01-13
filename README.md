# Truth Serum

Truth serum is a colloquial term for a variety of psychoactive drugs that are used to extract information from people who are unwilling or unable to provide it otherwise.

While LLMs consume a lot of information and are able to provide answers in a chat format and with some prompting, even structured formats, doing so is expensive and involves lots of matrix multiplication. The idea here is to extract everything that a model knows once into a knowledge graph and subsequenty use a cheaper graph query.

We'll be initially using sqlite. As the sqlalchemy engine for duckdb matures, we intend to switch to duckdb.

## Knowledge Graph Schema

A couple of blog posts with some of the motivations

* [Universal Knowledge Graph](https://adsharma.github.io/knowledge-graph-schema/)
* [Explainable AI](https://adsharma.github.io/explainable-ai/)

## Running it yourself
```
python3 truth/serum.py
# examine what you got
duckdb kg.db
.schema
select * from your table
```
