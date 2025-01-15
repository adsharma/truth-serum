from kg import graph


@graph
class Viewpoint:
    name: str
    description: str


@graph
class Ideas:
    title: str
    # Either a URL or <llm> <version> <prompt>
    source: str
    # Details about the idea
    description: str
