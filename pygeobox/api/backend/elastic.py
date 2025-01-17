###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

from copy import deepcopy
import logging

from elasticsearch import Elasticsearch, helpers
from typing import Tuple

from pygeobox.api.backend.base import BaseBackend
from pygeobox.util import datetime_days_ago

logging.getLogger('elasticsearch').setLevel(logging.ERROR)
LOGGER = logging.getLogger(__name__)

# default index settings
SETTINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'reportId': {
                'type': 'text',
                'fields': {
                    'raw': {
                        'type': 'keyword'
                    }
                }
            },
            'properties': {
                'properties': {
                    'resultTime': {
                        'type': 'date',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'pubTime': {
                        'type': 'date',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'phenomenonTime': {
                        'type': 'text'
                    },
                    'wigos_station_identifier': {
                        'type': 'text',
                        'fields': {
                            'raw': {'type': 'keyword'}
                        }
                    },
                    'value': {
                        'type': 'float',
                        'coerce': True
                    },
                    'metadata': {
                        'properties': {
                            'value': {
                                'type': 'float',
                                'coerce': True
                            }
                        }
                    }
                }
            }
        }
    }
}


class ElasticBackend(BaseBackend):
    """Elasticsearch API backend"""

    def __init__(self, defs: dict) -> None:
        """
        initializer

        :param defs: `dict` of connection parameters (RFC 1738 URL)
        """

        super().__init__(defs)

        self.type = 'Elasticsearch'
        self.url = defs.get('url').rstrip('/')

        self.conn = Elasticsearch([self.url], timeout=30,
                                  max_retries=10, retry_on_timeout=True)

    @staticmethod
    def es_id(collection_id: str) -> Tuple[str]:
        """
        Make collection_id ES friendly

        :param collection_id: `str` name of collection

        :returns: `str` of ES index
        """
        return collection_id.lower().replace(':', '-')

    def add_collection(self, collection_id: str) -> dict:
        """
        Add a collection

        :param collection_id: `str` name of collection

        :returns: `bool` of result
        """
        es_index = self.es_id(collection_id)

        if self.has_collection(collection_id):
            msg = f'index {es_index} exists'
            LOGGER.error(msg)
            raise RuntimeError(msg)

        settings = deepcopy(SETTINGS)

        LOGGER.debug('Creating index')
        self.conn.indices.create(index=es_index, body=settings)

        return self.has_collection(collection_id)

    def delete_collection(self, collection_id: str) -> bool:
        """
        Delete a collection

        :param collection_id: name of collection

        :returns: `bool` of delete result
        """
        es_index = self.es_id(collection_id)

        if not self.has_collection(collection_id):
            msg = f'index {es_index} does not exist'
            LOGGER.error(msg)
            raise RuntimeError(msg)

        if self.conn.indices.exists(es_index):
            self.conn.indices.delete(index=es_index)

        return not self.has_collection(collection_id)

    def has_collection(self, collection_id: str) -> bool:
        """
        Checks a collection

        :param collection_id: name of collection

        :returns: `bool` of collection result
        """
        es_index = self.es_id(collection_id)
        indices = self.conn.indices

        return indices.exists(es_index)

    def upsert_collection_items(self, collection_id: str, items: list) -> str:
        """
        Add or update collection items

        :param collection_id: name of collection
        :param items: list of GeoJSON item data `dict`'s

        :returns: `str` identifier of added item
        """
        es_index = self.es_id(collection_id)

        if not self.has_collection(collection_id):
            LOGGER.warning(f'Index {es_index} does not exist.  Creating')
            self.add_collection(es_index)

        def gendata(features):
            """
            Generator function to yield features
            """

            for feature in features:
                LOGGER.debug(f'Feature: {feature}')
                feature['properties']['id'] = feature['id']

                yield {
                    '_index': es_index,
                    '_id': feature['id'],
                    '_source': feature
                }

        helpers.bulk(self.conn, gendata(items))

    def delete_collection_item(self, collection_id: str, item_id: str) -> str:
        """
        Delete an item from a collection

        :param collection_id: name of collection
        :param item_id: `str` of item identifier

        :returns: `bool` of delete result
        """

        LOGGER.debug(f'Deleting {item_id} from {collection_id}')
        try:
            _ = self.conn.delete(index=collection_id, id=item_id)
        except Exception as err:
            msg = f'Item deletion failed: {err}'
            LOGGER.error(msg)
            raise RuntimeError(msg)

        return True

    def delete_collections_by_retention(self, days: int) -> bool:
        """
        Delete collections by retention date

        :param days: `int` of number of days

        :returns: `None`
        """

        indices = self.conn.indices.get('*').keys()

        before = datetime_days_ago(days)

        query_by_date = {
            'query': {
                'bool': {
                    'should': [{
                        'range': {
                            'properties.resultTime': {
                                'lte': before
                            }
                        }
                    }, {
                        'range': {
                            'properties.pubTime': {
                                'lte': before
                            }
                        }
                    }]
                }
            }
        }

        for index in indices:
            LOGGER.debug(f'deleting documents older than {days} days ({before})')  # noqa
            self.conn.delete_by_query(index=index, body=query_by_date)

        return

    def __repr__(self):
        return f'<ElasticBackend> (url={self.url})'
