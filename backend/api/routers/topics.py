"""Read-only semantic-topic registry and taxonomy navigation tree."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from adapters.storage import PostgresRawStorage
from api.deps import get_storage
from api.schemas import TaxonomyInfo, TaxonomyNode, TopicCatalog, TopicItem
from models.storage_models import TaxonomyNodeSnapshot


router = APIRouter(prefix="/api/topics", tags=["topics"])


def _node_response(node: TaxonomyNodeSnapshot) -> TaxonomyNode:
    return TaxonomyNode(
        slug=node.slug,
        name=node.display_name,
        topic_slugs=node.topic_slugs,
        children=[_node_response(child) for child in node.children],
    )


@router.get("", response_model=TopicCatalog)
async def get_topics(
    taxonomy: str = Query("default", min_length=1),
    storage: PostgresRawStorage = Depends(get_storage),
) -> TopicCatalog:
    catalog = await storage.get_taxonomy_catalog(taxonomy)
    if catalog is None:
        raise HTTPException(status_code=404, detail="Taxonomy not found")
    return TopicCatalog(
        taxonomy=TaxonomyInfo(
            slug=catalog.slug,
            name=catalog.display_name,
            revision=catalog.revision,
        ),
        topics=[
            TopicItem(
                id=topic.id,
                slug=topic.slug,
                name=topic.display_name,
                description=topic.description,
            )
            for topic in catalog.topics
        ],
        nodes=[_node_response(node) for node in catalog.nodes],
    )
