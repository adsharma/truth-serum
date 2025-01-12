from datetime import date

from kg import graph, property


@graph
class Event:
    name: str


@graph
class Date:
    date: date


@property
class EventStartDateRelation:
    pass


@property
class EventEndDateRelation:
    pass


@graph
class Conference:
    name: str


@property
class ConferenceEventRelation:
    pass


@property
class OrganizedByRelation:
    pass


@property
class AttendeeOfRelation:
    pass
