"""Test minimi su parser scraper e generazione sito."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from scraper import (  # noqa: E402
    _parse_periodo,
    _formato_iso,
    deduplica_lista,
    sanitizza_atto_per_archivio,
)
from genera_sito import genera_html  # noqa: E402


def test_parse_periodo():
    r = _parse_periodo("01/01/2025 - 15/01/2025")
    assert r["inizio"] == "2025-01-01"
    assert r["fine"] == "2025-01-15"


def test_formato_iso():
    assert _formato_iso("26/06/2026") == "2026-06-26"


def test_deduplica_placeholder_2031():
    atti = [
        {"numero_raw": "2026/1", "oggetto": "Test", "data_fine": "2031-12-31"},
        {"numero_raw": "2026/1", "oggetto": "Test", "data_fine": "2026-07-01"},
    ]
    out = deduplica_lista(atti)
    assert len(out) == 1
    assert out[0]["data_fine"] == "2026-07-01"


def test_sanitizza_atto():
    atto = {
        "numero_raw": "2026/1",
        "oggetto": "Oggetto",
        "riassunto": "Riassunto",
        "testo_combinato": "testo lungo pdf" * 1000,
        "allegati": [{"nome": "x.pdf"}],
        "cartella_locale": "data/allegati/x",
    }
    pulito = sanitizza_atto_per_archivio(atto)
    assert "testo_combinato" not in pulito
    assert "allegati" not in pulito
    assert pulito["oggetto"] == "Oggetto"


def test_genera_sito():
    atti = [{
        "tipo": "ATTI AMMINISTRATIVI/DELIBERA",
        "tipo_norm": "delibera",
        "numero_raw": "2026/100",
        "oggetto": "Approvazione bilancio",
        "data_inizio": "2026-08-01",
        "url_dettaglio": "https://example.com/atto",
        "riassunto": "Il comune approva il bilancio.",
    }]
    html_out = genera_html(atti)
    assert "Albo in chiaro" in html_out
    assert "Approvazione bilancio" in html_out
    assert "application/rss+xml" in html_out
    assert "embed-subscribe/albo-pretorio-pe" in html_out
