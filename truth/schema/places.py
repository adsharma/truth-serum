from kg import graph, property


@graph
class Country:
    name: str


@graph
class City:
    name: str


@graph
class Address:
    lat: float
    long: float


@graph
class Street:
    name: str


@graph
class StreetAddress:
    number: int


@graph
class PostalCode:
    number: int


@property
class CapitalRelation:
    pass


@property
class AddressStreetAddressRelation:
    pass


@property
class AddressCityRelation:
    pass


@property
class AddressPostalCodeRelation:
    pass


@property
class StreetAddressStreetRelation:
    pass
