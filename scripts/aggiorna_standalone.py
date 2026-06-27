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

    Il template è serializzato come stringa JSON con virgolette doppie come
    delimitatore esterno. I valori JS interni usano virgolette singole.
    Regole di escape nel contesto attuale (dentro stringa JSON):
      - backslash reale → \\\\ (4 backslash nel sorgente Python = \\ nel file)
      - apostrofo in valore JS → \\' (backslash+apostrofo nel file JSON)
      - newline logico tra campi → \\n (letterale nel file JSON)
      - virgolette doppie NON vanno usate nei valori (rompono il JSON esterno)
    """
    def sv(s: str) -> str:
        """
        Produce un letterale JS con virgolette singole, safe per stare
        dentro una stringa JSON con delimitatori doppi.

        Regole:
        - backslash → \\\\  (due backslash nel JSON = un backslash nel JS)
        - newline   → spazio (evita \\n letterali problematici)
        - apostrofo → \\u0027  (unicode escape: sicuro in JSON e in JS)
        - virgoletta doppia → lasciata as-is (non rompe la stringa JS con '')
        """
        s = str(s) if s else ""
        s = s.replace("\\", "\\\\")
        s = s.replace("\n", " ").replace("\r", "")
        s = s.replace("'", "\\u0027")   # apostrofo → unicode escape JSON-safe
        s = s.replace('"', "\\u0022")   # virgoletta doppia → unicode escape JSON-safe
        return f"'{s}'"

    righe = []
    for a in atti:
        tipo      = sv(tipo_breve(a.get("tipo", "Atto")))
        tipo_norm = sv(a.get("tipo_norm", ""))
        numero    = sv(a.get("numero_raw", ""))
        data      = sv(fmt_data(a.get("data_inizio", "")))
        dk        = sv((a.get("data_inizio", "") or "")[:10])
        oggetto   = sv((a.get("oggetto", "") or "")[:200])
        riassunto = sv((a.get("riassunto", "") or "")[:400])
        url       = sv(a.get("url_dettaglio", "") or "")

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
