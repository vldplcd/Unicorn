from abc import ABC, abstractmethod


class ModelElement(ABC):
    object_id = -1
    type = ""  # ANCHOR; TIE; KNOT; ATTRIBUTE
    name = "NA"
    description = "NA"

    def __init__(self, name):
        self.name = name
        self.object_id = hash(name) // 10


class Anchor(ModelElement):

    attributes = []

    def __init__(self, name, attributes):
        self.name = name
        self.object_id = hash(name) // 10
        self.attributes = attributes


class Attribute(ModelElement):
    type = "NA"
    parent_anchor = Anchor()
    parent_anchor_id = -2
    is_historized = False


class Tie(ModelElement):
    relations = []  #
    is_historized = False

    def __init__(self, name, relations):
        self.object_id = hash(name) // 10
        self.relations = relations


class Knot(ModelElement):
    connected_elements = []

    def __init__(self, name):
        self.object_id = hash(name) // 10


class Model:
    anchors = []
    ties = []
    knots = []


class Relation:
    name = ""
    type = ""  # N;1
    anchor = Anchor()
