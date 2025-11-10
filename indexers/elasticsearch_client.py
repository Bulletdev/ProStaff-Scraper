import os
from typing import Iterable, Dict
from elasticsearch import Elasticsearch, helpers


def get_es_url() -> str:
    url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    return url


def get_client() -> Elasticsearch:
    return Elasticsearch(get_es_url())


def ensure_index(name: str, mapping: Dict) -> None:
    es = get_client()
    if not es.indices.exists(index=name):
        es.indices.create(index=name, **mapping)


def bulk_index(index: str, docs: Iterable[Dict]) -> None:
    es = get_client()
    actions = (
        {
            "_index": index,
            "_id": doc.get("_id"),
            "_op_type": "index",
            "doc": doc,
            "doc_as_upsert": True,
        }
        for doc in docs
    )
    helpers.bulk(es, actions)