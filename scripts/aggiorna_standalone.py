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
        Produce un letterale JS con virgolette singole.

        Il blocco ALL_ATTI si trova DENTRO una stringa JSON con delimitatori
        doppi (il __bundler/template). Regole:
        - Il delimitatore del letterale JS deve essere ' (singolo)
          così le " non rompono il JSON esterno
        - L'apostrofo ' NON può stare raw dentro 'stringa' JS: spezzerebbe
          il letterale JS. Va escapato come \\' nel JS.
          Ma \\' in JSON sarebbe invalido (JSON non conosce \\')...
          SOLUZIONE: il blocco non è all'interno del JSON puro —
          è dentro il CONTENUTO della stringa JSON, quindi i backslash
          nel file sono interpretati dal JSON parser come escape.
          Nel file: \\' = due caratteri \\ e ' nel JSON raw.
          Il JSON parser legge \\ come un singolo \ e poi ' come apostrofo.
          Il risultato nel JS decoded è \' che è un escape JS valido.
          Quindi nel file raw dobbiamo scrivere \\\\' (4 chars = \\ nel JSON = \\ nel JS ??? NO)

          ANALISI CORRETTA del file originale:
          Il file originale aveva tipo: 'Determinazione Non Contabile'
          e gli apostrofi nei valori erano raw: L'ASSUNZIONE (funzionava!)

          Questo significa che il file originale aveva gli apostrofi raw
          dentro le stringhe JS singole. Come può funzionare in JSON?
          Perché ' non è un carattere speciale in JSON — è solo testo.

          Ma aspetta: il file originale di HEAD~1 aveva virgolette doppie
          e funzionava. Quello che stiamo generando ora ha virgolette singole
          come delimitatori. Gli apostrofi raw dentro 'valore' rompono il
          JS ma NON il JSON. Quindi il sito si carica ma il JS crasha.

          SOLUZIONE FINALE: usare virgolette singole come delimitatore,
          apostrofi escapati come \\u0027 (unicode escape - valido sia in
          JSON che in JS), virgolette doppie lasciate raw (safe in JSON
          esterno perché stiamo usando ' come delimitatore JS).

          Wait: virgolette doppie nel valore: tipo: 'valore "citato"'
          Nel file raw: tipo: 'valore "citato"'
          Il JSON vede: ...'valore " — la " chiude la stringa JSON esterna!

          SOLUZIONE DEFINITIVA: apostrofi → \\u0027, virgolette doppie → \\u0022
          Questi sono unicode escape validi in JSON e in JS.
        """
        s = str(s) if s else ""
        s = s.replace("\\", "\\\\")            # backslash reale → \\
        s = s.replace("\n", " ").replace("\r", "")
        # APOSTROFI: sia ASCII ' (0x27) che tipografico ' (U+2019, 0x2019)
        # Nel file raw: \\u0027 → JSON decode → ' → JS interpreta come '
        # Per U+2019 lo convertiamo direttamente in ' ASCII (safe in JS dentro '')
        s = s.replace("'", "\\\\u0027")        # apostrofo ASCII → \\u0027 nel file
        s = s.replace("’", "\\\\u0027")   # apostrofo tipografico → \\u0027
        s = s.replace("‘", "\\\\u0027")   # virgoletta sinistra ' → \\u0027
        # VIRGOLETTE DOPPIE: sia ASCII " che tipografiche " " (U+201C, U+201D)
        s = s.replace('"', "\\\\u0022")        # virgoletta doppia ASCII → \\u0022
        s = s.replace("“", "\\\\u0022")   # " sinistra tipografica
        s = s.replace("”", "\\\\u0022")   # " destra tipografica
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
