from kg import graph, property


@graph
class Topic:
    name: str


@graph
class Category:
    name: str


@property
class SubtopicOfRelation:
    pass


@property
class RelatedToRelation:
    pass
