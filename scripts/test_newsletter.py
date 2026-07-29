"""
test_newsletter.py — Invia una email di test via Buttondown.
Usare SOLO per verificare che l'integrazione funzioni.

Uso:
  BUTTONDOWN_API_KEY=xxx python scripts/test_newsletter.py
"""

import json
import os
import sys
from pathlib import Path

# Inietta atti fittizi in nuovi_atti.json e chiama publisher_email
NUOVI_ATTI_JSON = Path("data/nuovi_atti.json")

ATTI_TEST = [
    {
        "tipo": "Determinazione/Servizi",
        "numero_raw": "999/2026",
        "oggetto": "[TEST] Atto di prova per verifica newsletter automatica",
        "riassunto": (
            "Questo è un atto di test generato automaticamente per verificare "
            "che il sistema di invio newsletter via Buttondown funzioni correttamente. "
            "Nessun atto reale è stato pubblicato."
        ),
        "data_inizio": "2026-07-29",
        "url_dettaglio": "https://nt0wers84.github.io/albo-pretorio/",
    }
]

if not os.environ.get("BUTTONDOWN_API_KEY"):
    print("Errore: BUTTONDOWN_API_KEY non impostata.")
    sys.exit(1)

# Salva il backup del file originale se esiste
backup = None
if NUOVI_ATTI_JSON.exists():
    with open(NUOVI_ATTI_JSON) as f:
        backup = f.read()

try:
    NUOVI_ATTI_JSON.parent.mkdir(exist_ok=True)
    with open(NUOVI_ATTI_JSON, "w") as f:
        json.dump(ATTI_TEST, f)

    print("=== TEST NEWSLETTER ===")
    # Importa ed esegui il publisher
    import scripts.publisher_email as pe
    pe.main()

finally:
    # Ripristina il file originale
    if backup is not None:
        with open(NUOVI_ATTI_JSON, "w") as f:
            f.write(backup)
    elif NUOVI_ATTI_JSON.exists():
        NUOVI_ATTI_JSON.unlink()
    print("File nuovi_atti.json ripristinato.")
