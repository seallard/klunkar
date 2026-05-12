from datetime import date
from typing import Any, ClassVar

import httpx
import psycopg
from pydantic import BaseModel, ConfigDict

from klunkar.models import BaseSourcePayload, Source, Wine


class EnrichmentResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    sb_product_number: str
    confidence: float
    payload: dict[str, Any]


class Enricher:
    """Base class for review sources. Subclass and implement the abstract bits."""

    name: ClassVar[Source]
    display_name: ClassVar[str]
    payload_model: ClassVar[type[BaseSourcePayload]]

    def enrich_release(
        self,
        release_date: date,
        wines: list[Wine],
        client: httpx.Client,
        conn: psycopg.Connection,
    ) -> list[EnrichmentResult]:
        raise NotImplementedError

    def prepare_context(self, rows: list[tuple[Wine, dict[str, dict[str, Any]]]]) -> Any:
        """Compute release-level ranking context (e.g. Vivino's global mean).

        Default returns None — sources that don't need cross-wine context
        leave this alone.
        """
        return None

    def score(
        self,
        payload: BaseSourcePayload,
        wine: Wine,
        ctx: Any,
    ) -> tuple[float, tuple[Any, ...]]:
        """Return (rank_score, tiebreak_key) for this wine's payload."""
        raise NotImplementedError

    def render_row(self, payload: BaseSourcePayload) -> str:
        """Return one MarkdownV2-escaped row for format_message output."""
        raise NotImplementedError
