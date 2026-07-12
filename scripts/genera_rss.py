"""
genera_rss.py — Genera docs/feed.xml con gli atti degli ultimi 30 giorni.
Eseguito dal workflow GitHub Actions dopo aggiorna_standalone.py.

Feed URL: https://nt0wers84.github.io/albo-pretorio/feed.xml
"""

import json
import html
from datetime import date, timedelta, datetime
from pathlib import Path

ATTI_JSON   = Path("data/atti.json")
FEED_XML    = Path("docs/feed.xml")
FEED_URL    = "https://nt0wers84.github.io/albo-pretorio/feed.xml"
SITO_URL    = "https://nt0wers84.github.io/albo-pretorio/"
TITOLO_FEED = "Albo Pretorio — Pieve Emanuele"
DESCR_FEED  = "Atti pubblicati sull'Albo Pretorio del Comune di Pieve Emanuele"
GIORNI      = 30


def rfc822(data_iso: str) -> str:
    """Converte 'YYYY-MM-DD' in formato RFC-822 richiesto da RSS."""
    try:
        d = datetime.strptime(data_iso, "%Y-%m-%d")
    except ValueError:
        return ""
    # es. "Mon, 10 Jul 2026 00:00:00 +0000"
    return d.strftime("%a, %d %b %Y 00:00:00 +0000")


def tipo_display(atto: dict) -> str:
    tipo_raw = atto.get("tipo", "Atto")
    if "/" in tipo_raw:
        tipo_raw = tipo_raw.split("/")[-1].strip()
    return tipo_raw.title()


def genera_item(atto: dict) -> str:
    titolo = f"{tipo_display(atto)} n. {atto.get('numero_raw','?')} — {atto.get('oggetto','')}"
    link   = atto.get("url_dettaglio", SITO_URL)
    descr  = atto.get("riassunto") or atto.get("oggetto", "")
    data   = rfc822(atto.get("data_inizio", ""))
    guid   = atto.get("id_atto") or atto.get("url_dettaglio") or titolo

    return (
        "    <item>\n"
        f"      <title>{html.escape(titolo)}</title>\n"
        f"      <link>{html.escape(link)}</link>\n"
        f"      <description>{html.escape(descr)}</description>\n"
        f"      <guid isPermaLink=\"false\">{html.escape(guid)}</guid>\n"
        + (f"      <pubDate>{data}</pubDate>\n" if data else "")
        + "    </item>"
    )


def main():
    if not ATTI_JSON.exists():
        print("data/atti.json non trovato — feed.xml non generato.")
        return

    with open(ATTI_JSON, encoding="utf-8") as f:
        atti = json.load(f)

    cutoff = (date.today() - timedelta(days=GIORNI)).isoformat()
    recenti = [
        a for a in atti
        if (a.get("data_inizio") or "") >= cutoff
    ]
    # Più recente prima
    recenti.sort(key=lambda a: a.get("data_inizio", ""), reverse=True)

    items_xml = "\n".join(genera_item(a) for a in recenti)

    feed = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">\n'
        '  <channel>\n'
        f'    <title>{html.escape(TITOLO_FEED)}</title>\n'
        f'    <link>{SITO_URL}</link>\n'
        f'    <description>{html.escape(DESCR_FEED)}</description>\n'
        '    <language>it</language>\n'
        f'    <atom:link href="{FEED_URL}" rel="self" type="application/rss+xml"/>\n'
        + (f"\n{items_xml}\n" if items_xml else "")
        + '  </channel>\n'
        '</rss>\n'
    )

    FEED_XML.parent.mkdir(exist_ok=True)
    FEED_XML.write_text(feed, encoding="utf-8")
    print(f"feed.xml generato: {len(recenti)} atti (ultimi {GIORNI} giorni).")


if __name__ == "__main__":
    main()
