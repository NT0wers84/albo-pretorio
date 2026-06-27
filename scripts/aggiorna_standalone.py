"""
aggiorna_standalone.py — Inietta gli atti aggiornati nel file standalone HTML.

Il file 'docs/Albo Pretorio Standalone.html' è un bundle autocontenuto
generato da Claude Design. I dati degli atti sono embedded come stringa
JS-escaped dentro il tag <script type="__bundler/template">, nella variabile:

  ALL_ATTI = [
    { tipo: '...', tipoNorm: '...', numero: '...', data: '...', dk: '...',
      oggetto: '...', riassunto: '...', url: '...' },
    ...
  ];

Questo script:
1. Legge data/atti.json (generato da scraper.py)
2. Converte gli atti nel formato JS-escaped atteso dal bundle
3. Sostituisce il blocco ALL_ATTI nel file standalone
4. Salva il file aggiornato

Viene chiamato dal workflow GitHub Actions dopo scraper.py.
"""

import json
import re
from pathlib import Path
from datetime import datetime

ATTI_JSON  = Path("data/atti.json")
STANDALONE = Path("docs/index.html")


def fmt_data(iso: str) -> str:
    """Converte '2026-06-26' in '26/06/2026'."""
    if not iso or len(iso) < 10:
        return iso or ""
    y, m, d = iso[:10].split("-")
    return f"{d}/{m}/{y}"


def tipo_breve(tipo_raw: str) -> str:
    """Prende la parte dopo '/' e la mette in Title Case."""
    if "/" in tipo_raw:
        return tipo_raw.split("/")[-1].strip().title()
    return tipo_raw.strip().title()


def atti_to_js_block(atti: list[dict]) -> str:
    """
    Converte la lista degli atti nel blocco JS atteso dal bundle.

    Il bundle contiene il template come stringa JSON-escaped, quindi
    i newline reali sono \\n e le virgolette sono escaped.
    Usiamo json.dumps() per ogni valore stringa: produce escape sicuri
    (es. l'apostrofo rimane apostrofo, le virgolette diventano \\").
    Poi rimuoviamo le virgolette esterne di json.dumps e usiamo
    virgolette doppie come delimitatori JS — compatibili con JSON.
    """
    def jv(s: str) -> str:
        """Serializza un valore stringa come letterale JS con virgolette doppie."""
        # json.dumps produce "stringa" con escape corretti per tutti i caratteri
        return json.dumps(str(s) if s else "", ensure_ascii=False)

    righe = []
    for a in atti:
        tipo      = jv(tipo_breve(a.get("tipo", "Atto")))
        tipo_norm = jv(a.get("tipo_norm", ""))
        numero    = jv(a.get("numero_raw", ""))
        data      = jv(fmt_data(a.get("data_inizio", "")))
        dk        = jv((a.get("data_inizio", "") or "")[:10])
        oggetto   = jv((a.get("oggetto", "") or "")[:200])
        riassunto = jv((a.get("riassunto", "") or "")[:400])
        url       = jv(a.get("url_dettaglio", "") or "")

        riga = (
            f"    {{ tipo: {tipo}, tipoNorm: {tipo_norm}, "
            f"numero: {numero}, data: {data}, dk: {dk},\\n"
            f"      oggetto: {oggetto},\\n"
            f"      riassunto: {riassunto},\\n"
            f"      url: {url} }}"
        )
        righe.append(riga)

    corpo = ",\\n".join(righe)
    return f"ALL_ATTI = [\\n{corpo},\\n  ]"


def aggiorna_standalone(atti: list[dict]) -> bool:
    """
    Sostituisce il blocco ALL_ATTI nel file standalone.
    Restituisce True se il file è stato aggiornato.
    """
    if not STANDALONE.exists():
        print(f"File non trovato: {STANDALONE}")
        return False

    with open(STANDALONE, "r", encoding="utf-8") as f:
        content = f.read()

    # Trova il blocco ALL_ATTI = [ ... ];
    # Nel file il contenuto è dentro una stringa JSON, quindi i newline
    # sono \\n (due caratteri backslash+n, non newline reale)
    pattern = re.compile(r"ALL_ATTI = \[.*?\]", re.DOTALL)
    match = pattern.search(content)

    if not match:
        print("ERRORE: blocco ALL_ATTI non trovato nel file standalone.")
        print("Il file potrebbe avere una struttura diversa dal previsto.")
        return False

    print(f"Blocco trovato: posizione {match.start()}–{match.end()}")
    print(f"Atti da iniettare: {len(atti)}")

    nuovo_blocco = atti_to_js_block(atti)
    nuovo_content = content[:match.start()] + nuovo_blocco + content[match.end():]

    with open(STANDALONE, "w", encoding="utf-8") as f:
        f.write(nuovo_content)

    print(f"File aggiornato: {STANDALONE} ({len(nuovo_content)//1024} KB)")
    return True


def main():
    if not ATTI_JSON.exists():
        print(f"File non trovato: {ATTI_JSON}")
        return

    with open(ATTI_JSON, "r", encoding="utf-8") as f:
        atti = json.load(f)

    print(f"Caricati {len(atti)} atti da {ATTI_JSON}")
    ok = aggiorna_standalone(atti)

    if ok:
        print(f"Aggiornamento completato: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    else:
        print("Aggiornamento fallito.")


if __name__ == "__main__":
    main()
