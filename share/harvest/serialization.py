import json
import logging

logger = logging.getLogger(__name__)


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


class EverythingSerializer(RawDatumSerializer):
    def __init__(self, pretty: bool=False):
        super().__init__(pretty=pretty)
        self.dict_serializer = DictSerializer(pretty=pretty)
        logger.warning('%r is deprecated. Use a serializer meant for the data returned', self)

    def serialize(self, data, pretty=False) -> str:
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            logger.warning(
                '%r.encode_data got a bytes instance. '
                'do_harvest should be returning str types as only the harvester will know how to properly encode the bytes'
                'defaulting to decoding as utf-8',
                self,
            )
            return data.decode('utf-8')
        if isinstance(data, dict):
            return self.dict_serializer.serialize(data)
        raise Exception('Unable to properly encode data blob {!r}. Data should be a dict, bytes, or str objects.'.format(data))
