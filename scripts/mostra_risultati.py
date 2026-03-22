"""
mostra_risultati.py — Stampa statistiche sugli atti trovati nell'ultima esecuzione.
Chiamato dal workflow GitHub Actions dopo lo scraper.
"""
import json
import os

f = "data/nuovi_atti.json"

if not os.path.exists(f):
    print("Nessun file di output trovato.")
else:
    with open(f, "r", encoding="utf-8") as fp:
        atti = json.load(fp)
    print(f"Atti nuovi trovati: {len(atti)}")
    for a in atti[:10]:
        tipo = a.get("tipo", "?")
        oggetto = a.get("oggetto", "?")[:70]
        print(f"  [{tipo}] {oggetto}")
