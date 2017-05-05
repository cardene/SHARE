import json


class RawDatumSerializer:
    """A deterministic serializer for harvested data.
    """

    def __init__(self, pretty: bool=False):
        self.pretty = pretty

    def serialize(self, value) -> str:
        raise NotImplementedError()


class DictSerializer(RawDatumSerializer):

    def serialize(self, value: dict) -> str:
        return json.dumps(value, sort_keys=True, indent=4 if self.pretty else None)
