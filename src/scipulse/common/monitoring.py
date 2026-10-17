"""Envoi de métriques pipeline (volumes, résultats GE, fraîcheur) vers Logstash → ES."""
from datetime import datetime, timezone

import requests

from scipulse.common.logging import get_logger
from scipulse.config import settings

log = get_logger(__name__)


def emit(event: str, source: str, layer: str, **fields) -> None:
    """Exemple : emit("ingestion", source="hn", layer="bronze", records=120)."""
    payload = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "source": source,
        "layer": layer,
        **fields,
    }
    try:
        requests.post(settings.logstash_http, json=payload, timeout=5)
    except requests.RequestException as exc:  # le monitoring ne doit jamais casser le pipeline
        log.warning("Monitoring indisponible: %s", exc)
