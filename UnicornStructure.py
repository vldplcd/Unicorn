from abc import ABC, abstractmethod
import json


class ModelElement(ABC):
    object_id = -1
    type = ""  # ANCHOR; TIE; KNOT; ATTRIBUTE
    name = "NA"
    description = "NA"

    def __init__(self, data: str):
        self.__dict__ = json.loads(data)

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class Anchor(ModelElement):

    attributes = []

    def __init__(self, data: str):
        self.__dict__ = json.loads(data)
        temp_attr = self.attributes.copy()
        self.attributes = []
        for attr in temp_attr:
            self.attributes.append(Attribute(json.dumps(attr)))


class Attribute(ModelElement):
    value_type = "NA"
    parent_anchor_id = -2
    is_historized = False


class Tie(ModelElement):
    relations = []  #
    is_historized = False

    def __init__(self, data: str):
        self.__dict__ = json.loads(data)

class Knot(ModelElement):
    connected_elements = []


class Model(object):
    anchors = []
    ties = []
    knots = []

    def __init__(self, data: str):
        data_dict = json.loads(data)
        for anch in data_dict['anchors']:
            self.anchors.append(Anchor(json.dumps(anch)))

        for tie in data_dict['ties']:
            self.anchors.append(Tie(json.dumps(tie)))


class Relation(object):
    name = ""
    type = ""  # N;1
    anchor_id = -1
