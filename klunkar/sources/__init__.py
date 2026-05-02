from klunkar.models import Source
from klunkar.sources.base import Enricher, EnrichmentResult
from klunkar.sources.munskankarna import MunskankarnaEnricher
from klunkar.sources.vivino import VivinoEnricher

ENRICHERS: dict[Source, Enricher] = {e.name: e for e in (VivinoEnricher(), MunskankarnaEnricher())}

__all__ = ["Enricher", "EnrichmentResult", "ENRICHERS"]
