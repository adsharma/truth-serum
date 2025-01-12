from kg import graph, property


@graph
class Country:
    name: str
    id: int | None = None


@graph
class City:
    name: str
    id: int | None = None


@property
class CapitalRelation:
    pass
