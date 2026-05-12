"""Lock the Enricher contract.

Every registered enricher must declare the class-level attributes
(`name`, `display_name`, `payload_model`) and implement `enrich_release`,
`score`, and `render_row`. A new source author who forgets one of
these — easy to do in a single-file PR — will fail this test in CI
instead of silently breaking ranking or notifications.
"""

from klunkar.models import BaseSourcePayload, Source
from klunkar.sources import ENRICHERS
from klunkar.sources.base import Enricher


def test_every_source_enum_value_has_a_registered_enricher():
    assert set(ENRICHERS) == set(Source)


def test_each_enricher_subclasses_the_base():
    for enricher in ENRICHERS.values():
        assert isinstance(enricher, Enricher)


def test_each_enricher_has_required_class_attributes():
    for source, enricher in ENRICHERS.items():
        assert enricher.name is source
        assert isinstance(enricher.display_name, str) and enricher.display_name
        assert issubclass(enricher.payload_model, BaseSourcePayload)


def test_each_enricher_overrides_score_and_render_row():
    for enricher in ENRICHERS.values():
        cls = type(enricher)
        # The methods must be overridden — the base raises NotImplementedError.
        assert cls.score is not Enricher.score, f"{cls.__name__} did not override score()"
        assert cls.render_row is not Enricher.render_row, (
            f"{cls.__name__} did not override render_row()"
        )
        assert cls.enrich_release is not Enricher.enrich_release, (
            f"{cls.__name__} did not override enrich_release()"
        )
