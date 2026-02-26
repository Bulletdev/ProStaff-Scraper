import os
from typing import Iterable, Dict, List
from urllib.parse import urlparse
from elasticsearch import Elasticsearch, helpers


def get_es_url() -> str:
    return os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def get_client() -> Elasticsearch:
    """Build an Elasticsearch client from ELASTICSEARCH_URL.

    Credentials are resolved in this order:
    1. Embedded in the URL:  http://user:pass@host:9200
    2. Separate env vars:    ELASTICSEARCH_USER / ELASTICSEARCH_PASSWORD
    3. No auth (plain URL)
    """
    url = get_es_url()
    parsed = urlparse(url)

    if parsed.username and parsed.password:
        clean_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        return Elasticsearch(
            clean_url,
            basic_auth=(parsed.username, parsed.password),
            verify_certs=False,
        )

    user = os.getenv("ELASTICSEARCH_USER")
    password = os.getenv("ELASTICSEARCH_PASSWORD")
    if user and password:
        return Elasticsearch(
            url,
            basic_auth=(user, password),
            verify_certs=False,
        )

    return Elasticsearch(url)


def ensure_index(name: str, mapping: Dict) -> None:
    es = get_client()
    if not es.indices.exists(index=name):
        es.indices.create(index=name, **mapping)


def bulk_index(index: str, docs: Iterable[Dict]) -> None:
    es = get_client()
    actions = []
    for doc in docs:
        doc = dict(doc)
        doc_id = doc.pop("_id", None)
        actions.append({
            "_index": index,
            "_id": doc_id,
            "_source": doc,
        })
    helpers.bulk(es, actions)


def update_document(index: str, doc_id: str, fields: Dict) -> None:
    """Partially update an existing document by ID."""
    es = get_client()
    es.update(index=index, id=doc_id, doc=fields)


def query_unenriched(index: str, size: int = 50) -> List[Dict]:
    """Return documents where riot_enriched is false, ordered by start_time asc.

    Each returned item is a dict with '_id' and '_source' keys.
    """
    es = get_client()
    query = {
        "query": {"term": {"riot_enriched": False}},
        "size": size,
        "sort": [{"start_time": {"order": "asc", "unmapped_type": "date"}}],
    }
    result = es.search(index=index, **query)
    return [
        {"_id": hit["_id"], "_source": hit["_source"]}
        for hit in result["hits"]["hits"]
    ]