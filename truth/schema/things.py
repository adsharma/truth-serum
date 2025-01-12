from kg import graph, property


@graph
class Building:
    name: str


@property
class LocatedAtRelation:
    pass


@property
class BuiltDateRelation:
    pass


@graph
class Monument:
    name: str


@property
class MonumentBuildingRelation:
    pass


@property
class ArchitectOfRelation:
    pass


@property
class BuiltByRelation:
    pass
