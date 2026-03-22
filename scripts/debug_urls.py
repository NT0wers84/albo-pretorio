"""
debug_urls.py — Prova tutti gli endpoint noti di JCityGov/Liferay
per trovare quello che restituisce la lista degli atti.

Eseguito da GitHub Actions come test diagnostico.
"""

import requests
from bs4 import BeautifulSoup

BASE = "https://pieveemanuele.trasparenza-valutazione-merito.it"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
})

# Tutti gli URL da provare — coprono i vari pattern usati dalla piattaforma JCityGov
URLS_DA_PROVARE = [
    # URL base albo pretorio
    ("Pagina principale", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"),

    # URL alternativi per l'albo
    ("Albo pretorio (alternativo)", f"{BASE}/web/trasparenza/albo-pretorio"),
    ("Papca AP", f"{BASE}/web/trasparenza/papca-ap"),
    ("Storico atti", f"{BASE}/web/trasparenza/storico-atti"),

    # Portlet JCityGov — render normale
    ("Portlet albo (render)", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"),

    # Portlet — stato esclusivo (solo contenuto portlet, senza layout)
    ("Portlet albo (exclusive)", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=0&p_p_state=exclusive&p_p_mode=view"),

    # Portlet — resource URL per export CSV
    ("Export CSV", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view"
     "&p_p_resource_id=exportList"
     "&_jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet_format=csv"),

    # Portlet — resource URL per export PDF
    ("Export PDF", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view"
     "&p_p_resource_id=exportList"
     "&_jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet_format=pdf"),

    # Menu trasversale (navigazione laterale)
    ("Menu trasversale", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovmenutrasversaleleftcolumn_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"),

    # Menu trasversale esclusivo
    ("Menu trasversale (exclusive)", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovmenutrasversaleleftcolumn_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=0&p_p_state=exclusive&p_p_mode=view"),

    # PAPCA con igrid (formato alternativo usato da alcuni comuni)
    ("PAPCA igrid", f"{BASE}/web/trasparenza/papca-ap/-/papca/igrid/0/Albo_pretorio/"),
    ("PAPCA igrid v2", f"{BASE}/web/trasparenza/papca-ap/-/papca/list"),

    # API JSON (alcuni portali Maggioli lo espongono)
    ("API JSON atti", f"{BASE}/api/jsonws/albo-pretorio-portlet"),
    ("API JSON v2", f"{BASE}/api/jsonws/JCityGov_AlbiPortlet"),

    # Liferay JSON web services
    ("Liferay JSONWS", f"{BASE}/api/jsonws"),

    # Portlet con paginazione esplicita
    ("Portlet con delta", f"{BASE}/web/trasparenza/dettaglio-albo-pretorio"
     "?p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
     "&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"
     "&_jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet_cur=1"
     "&_jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet_delta=20"),

    # Endpoint c/ con layout ID diversi
    ("Layout c/ ID", f"{BASE}/c/portal/layout"
     "?p_l_id=0&p_p_id=jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"),
]


def prova_url(nome: str, url: str):
    """Prova un URL e stampa un report sintetico."""
    try:
        resp = SESSION.get(url, timeout=15, allow_redirects=True)
        status = resp.status_code
        content_type = resp.headers.get("Content-Type", "?")
        lunghezza = len(resp.text)

        # Cerca indicatori di contenuto utile
        soup = BeautifulSoup(resp.text, "html.parser")
        tabelle = len(soup.find_all("table"))
        has_delibera = "delibera" in resp.text.lower()
        has_ordinanza = "ordinanza" in resp.text.lower()
        has_determina = "determina" in resp.text.lower()
        has_blocked = "blocked" in resp.text.lower() or "bloccata" in resp.text.lower()

        # Risultato
        indicatori = []
        if tabelle > 0:
            indicatori.append(f"{tabelle} tabelle")
        if has_delibera:
            indicatori.append("DELIBERA")
        if has_ordinanza:
            indicatori.append("ORDINANZA")
        if has_determina:
            indicatori.append("DETERMINA")
        if has_blocked:
            indicatori.append("⚠ BLOCCATO")

        stato_emoji = "✅" if status == 200 and not has_blocked else "❌" if status >= 400 else "⚠️"
        ind_str = " | ".join(indicatori) if indicatori else "nessun contenuto rilevante"

        print(f"\n{stato_emoji} [{status}] {nome}")
        print(f"   URL: {url[:120]}...")
        print(f"   Content-Type: {content_type} | {lunghezza} char")
        print(f"   Contenuto: {ind_str}")

        # Se trovato contenuto rilevante, mostra un estratto
        if any([has_delibera, has_ordinanza, has_determina]) and not has_blocked:
            print(f"   ★★★ CONTENUTO TROVATO! ★★★")
            # Mostra il testo visibile
            testo = soup.get_text(separator=" ", strip=True)
            # Cerca la prima occorrenza di parole chiave
            for kw in ["delibera", "ordinanza", "determina"]:
                pos = testo.lower().find(kw)
                if pos >= 0:
                    estratto = testo[max(0, pos-50):pos+200]
                    print(f"   Estratto: ...{estratto}...")
                    break

        if tabelle > 0:
            print(f"   ★★★ TABELLE TROVATE! ★★★")
            for t in soup.find_all("table")[:3]:
                righe = t.find_all("tr")
                print(f"   Tabella: {len(righe)} righe")
                if righe:
                    prima_riga = righe[0].get_text(separator=" | ", strip=True)
                    print(f"   Prima riga: {prima_riga[:150]}")

    except Exception as e:
        print(f"\n❌ {nome}")
        print(f"   URL: {url[:120]}...")
        print(f"   Errore: {e}")


def main():
    print("=" * 70)
    print("DEBUG URLS — Probing endpoint JCityGov Pieve Emanuele")
    print("=" * 70)

    for nome, url in URLS_DA_PROVARE:
        prova_url(nome, url)

    print("\n" + "=" * 70)
    print("FINE PROBING")
    print("=" * 70)


if __name__ == "__main__":
    main()
