from kg import graph, property


@graph
class Person:
    name: str


@property
class BirthDateRelation:
    pass


@property
class BirthPlaceRelation:
    pass


@property
class ResidesInRelation:
    pass


@property
class DeathDateRelation:
    pass


@graph
class Organization:
    name: str


@property
class FoundedDateRelation:
    pass


@property
class MemberOfRelation:
    pass


@property
class FounderOfRelation:
    pass
