"""
scraper.py — Albo Pretorio Comune di Pieve Emanuele
Piattaforma: JCityGov di Maggioli (Liferay)
Eseguito da GitHub Actions ogni giorno alle 08:00 (cron 0 6 * * *)
"""

import os
import re
import json
import time
import logging
import requests
from groq import Groq
import pdfplumber
from pathlib import Path
from datetime import date, datetime
from bs4 import BeautifulSoup

# ── Configurazione OCR (opzionale, attivato automaticamente) ──────────────────
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_DISPONIBILE = True
except ImportError:
    OCR_DISPONIBILE = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Costanti ──────────────────────────────────────────────────────────────────
BASE_URL = "https://pieveemanuele.trasparenza-valutazione-merito.it"
# URL corretto: il sito usa il modulo PAPCA (non dettaglio-albo-pretorio)
# Confermato dal test debug: restituisce tabella HTML statica, nessun JS necessario
ALBO_URL = f"{BASE_URL}/web/trasparenza/papca-ap/-/papca/igrid/0/Albo_pretorio/"

# Percorsi file
DATA_DIR       = Path("data")
ALLEGATI_DIR   = DATA_DIR / "allegati"
ATTI_JSON      = DATA_DIR / "atti.json"
NUOVI_ATTI_JSON = DATA_DIR / "nuovi_atti.json"

# Limite allegati per atto
MAX_ALLEGATI = 10

# Soglia OCR: se una pagina PDF ha meno di 50 caratteri → attiva Tesseract
SOGLIA_OCR = 50

# ── Filtri — categorie di atti da monitorare ──────────────────────────────────
TIPI_INCLUSI = [
    "delibera",
    "determinazione",
    "ordinanza",
    "avviso",
    "bando",
    "appalto",
    "gara",
    "variazione di bilancio",
    "variazione bilancio",
]

# Parole chiave nell'oggetto che identificano atti da ESCLUDERE
OGGETTI_ESCLUSI = [
    "pubblicazione matrimonio",
    "pubblicazioni di matrimonio",
    "cambio nome",
    "cambio cognome",
    "rettifica nome",
    "rettifica cognome",
]

# ── Sessione HTTP (per download PDF e risorse statiche) ───────────────────────
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
})
# Retry automatico su timeout e errori di connessione (3 tentativi, backoff 2s)
_retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
SESSION.mount("https://", HTTPAdapter(max_retries=_retry))
SESSION.mount("http://", HTTPAdapter(max_retries=_retry))


def _init_sessione() -> None:
    """
    Visita la pagina principale del portale per ottenere i cookie di sessione Liferay
    (JSESSIONID, GUEST_LANGUAGE_ID, ecc.) necessari per accedere agli endpoint
    recuperaDettaglio e downloadAllegato.
    """
    try:
        # 1. Homepage del portale trasparenza
        r1 = SESSION.get(f"{BASE_URL}/web/trasparenza", timeout=30)
        log.info(f"Sessione inizializzata: {r1.status_code}, cookie: {list(SESSION.cookies.keys())}")
        # 2. Pagina dell'albo (stabilisce il contesto del portlet)
        r2 = SESSION.get(ALBO_URL, timeout=30)
        log.info(f"Contesto portlet albo: {r2.status_code}, cookie: {list(SESSION.cookies.keys())}")
    except Exception as e:
        log.warning(f"Inizializzazione sessione fallita: {e}")


def _fetch(url: str) -> str:
    """Scarica una pagina con requests (nessun JS necessario per PAPCA)."""
    resp = SESSION.get(url, timeout=90)
    resp.raise_for_status()
    return resp.text


# ─────────────────────────────────────────────────────────────────────────────
# 1. SCRAPING LISTA ATTI
# ─────────────────────────────────────────────────────────────────────────────

def scrape_lista_atti() -> list[dict]:
    """
    Legge tutte le pagine dell'albo pretorio e restituisce la lista grezza
    degli atti con: numero, tipo, oggetto, date, url_dettaglio.
    """
    atti = []
    url_corrente = ALBO_URL
    pagina = 1

    while url_corrente:
        log.info(f"Scarico pagina {pagina}: {url_corrente}")

        try:
            html = _fetch(url_corrente)
        except requests.RequestException as e:
            log.error(f"Errore HTTP sulla pagina {pagina}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")

        # Cerca la tabella degli atti
        tabella = soup.find("table")
        if not tabella:
            log.warning(f"Nessuna tabella trovata a pagina {pagina} anche dopo JS. Fine elenco.")
            break

        # Intestazioni per mappare le colonne
        intestazioni = [th.get_text(strip=True) for th in tabella.find_all("th")]
        log.debug(f"Intestazioni tabella: {intestazioni}")

        # Mappa indici colonne (gestisce variazioni nel testo intestazione)
        idx = _trova_indici_colonne(intestazioni)

        righe = tabella.find_all("tr")[1:]  # salta intestazione
        for riga in righe:
            celle = riga.find_all("td")
            if len(celle) < max(idx.values()) + 1:
                continue

            atto = _estrai_atto_da_riga(celle, idx, soup, riga)
            if atto:
                atti.append(atto)

        # Paginazione: cerca il link "Avanti"
        url_corrente = _trova_link_avanti(soup, url_corrente)
        pagina += 1

        time.sleep(1)  # rispetto del server

    log.info(f"Totale atti trovati nell'albo: {len(atti)}")
    return atti


def _trova_indici_colonne(intestazioni: list[str]) -> dict:
    """
    Mappa nomi colonne a indici numerici. Tollerante a variazioni nel testo.
    """
    idx = {"numero": 0, "tipo": 1, "oggetto": 2, "periodo": 3}

    for i, h in enumerate(intestazioni):
        h_lower = h.lower()
        if "numero" in h_lower or "registro" in h_lower:
            idx["numero"] = i
        elif "tipo" in h_lower:
            idx["tipo"] = i
        elif "oggetto" in h_lower:
            idx["oggetto"] = i
        elif "periodo" in h_lower or "pubblicazion" in h_lower:
            idx["periodo"] = i

    return idx


def _estrai_atto_da_riga(celle, idx: dict, soup, riga) -> dict | None:
    """
    Estrae i metadati di un singolo atto dalla riga della tabella.
    Cerca il link "Apri Dettaglio" nella riga stessa o tramite attributo title.
    """
    try:
        numero_raw = celle[idx["numero"]].get_text(strip=True)
        tipo       = celle[idx["tipo"]].get_text(strip=True)
        oggetto    = celle[idx["oggetto"]].get_text(strip=True)
        periodo    = celle[idx["periodo"]].get_text(strip=True)

        # Parsing date: "01/01/2025 - 15/01/2025" oppure "01/01/2025 15/01/2025"
        date_pub = _parse_periodo(periodo)

        # Link al dettaglio (attributo title="Apri Dettaglio" oppure nella cella)
        link_tag = riga.find("a", title="Apri Dettaglio")
        if not link_tag:
            # Fallback: cerca qualsiasi <a> nella riga
            link_tag = riga.find("a", href=True)

        url_dettaglio = None
        if link_tag and link_tag.get("href"):
            href = link_tag["href"]
            if href.startswith("http"):
                url_dettaglio = href
            else:
                url_dettaglio = BASE_URL + href

        return {
            "numero_raw": numero_raw,
            "tipo": tipo,
            "oggetto": oggetto,
            "data_inizio": date_pub.get("inizio"),
            "data_fine": date_pub.get("fine"),
            "url_dettaglio": url_dettaglio,
        }

    except Exception as e:
        log.warning(f"Errore nell'estrarre riga: {e}")
        return None


def _parse_periodo(periodo: str) -> dict:
    """
    Parsa una stringa tipo '01/01/2025 - 15/01/2025' o '01/01/2025 15/01/2025'.
    Restituisce {'inizio': 'YYYY-MM-DD', 'fine': 'YYYY-MM-DD'}.
    """
    date_trovate = re.findall(r"\d{2}/\d{2}/\d{4}", periodo)
    result = {"inizio": None, "fine": None}

    if len(date_trovate) >= 1:
        result["inizio"] = _formato_iso(date_trovate[0])
    if len(date_trovate) >= 2:
        result["fine"] = _formato_iso(date_trovate[1])

    return result


def _formato_iso(data_it: str) -> str:
    """Converte 'DD/MM/YYYY' in 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(data_it, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return data_it


def _trova_link_avanti(soup: BeautifulSoup, url_corrente: str) -> str | None:
    """
    Cerca il link 'Avanti' nella paginazione.
    Il portale JCityGov usa: <div class="pagination pagination-centered">
    """
    paginazione = soup.find("div", class_="pagination pagination-centered")
    if not paginazione:
        # Fallback: cerca qualsiasi link "Avanti" o ">" nella pagina
        for link in soup.find_all("a"):
            testo = link.get_text(strip=True)
            if testo in ("Avanti", "»", "›", "Next", ">"):
                href = link.get("href", "")
                if href and href != "#":
                    return href if href.startswith("http") else BASE_URL + href
        return None

    for link in paginazione.find_all("a"):
        if link.get_text(strip=True) == "Avanti":
            href = link.get("href", "")
            if href and href != "#" and href != url_corrente:
                return href if href.startswith("http") else BASE_URL + href

    return None


# ─────────────────────────────────────────────────────────────────────────────
# 2. FILTRI
# ─────────────────────────────────────────────────────────────────────────────

def applica_filtri(atti: list[dict]) -> list[dict]:
    """
    Filtra gli atti mantenendo solo quelli rilevanti per il monitoraggio civico.
    """
    filtrati = []
    for atto in atti:
        tipo    = (atto.get("tipo") or "").lower()
        oggetto = (atto.get("oggetto") or "").lower()

        # Escludi per oggetto
        if any(ex in oggetto for ex in OGGETTI_ESCLUSI):
            continue

        # Includi per tipo
        if any(inc in tipo for inc in TIPI_INCLUSI):
            filtrati.append(atto)

    log.info(f"Atti filtrati (rilevanti): {len(filtrati)} su {len(atti)} totali")
    return filtrati


# ─────────────────────────────────────────────────────────────────────────────
# 3. DEDUPLICAZIONE (solo atti nuovi rispetto all'archivio)
# ─────────────────────────────────────────────────────────────────────────────

def deduplica_lista(atti: list[dict]) -> list[dict]:
    """
    Rimuove i duplicati dalla lista degli atti scrappati.
    Il portale PAPCA pubblica lo stesso atto due volte con URL diversi:
    una riga con data_fine=2031-12-31 (pubblicazione permanente)
    e una con la data di scadenza effettiva.
    Teniamo quella con la data_fine più recente (≤ oggi + qualche anno),
    identificando i duplicati tramite (numero_raw, oggetto).
    """
    visti: dict[tuple, dict] = {}
    for atto in atti:
        chiave = (atto.get("numero_raw", ""), atto.get("oggetto", ""))
        if chiave not in visti:
            visti[chiave] = atto
        else:
            # Tieni quello con data_fine più utile (non 2031-12-31)
            esistente = visti[chiave]
            fine_nuova    = atto.get("data_fine") or ""
            fine_esistente = esistente.get("data_fine") or ""
            # Preferisci la data fine < 2030 (data reale vs placeholder)
            if fine_esistente > "2030" and fine_nuova < "2030":
                visti[chiave] = atto

    risultato = list(visti.values())
    if len(risultato) < len(atti):
        log.info(f"Deduplicati {len(atti) - len(risultato)} atti doppi dal portale")
    return risultato


def filtra_nuovi(atti: list[dict]) -> list[dict]:
    """
    Restituisce solo gli atti non ancora presenti in data/atti.json.
    Usa (numero_raw, oggetto) come identificatore univoco (più robusto dell'URL
    che cambia ad ogni sessione per via del token p_auth).
    """
    atti_noti: set[tuple] = set()
    if ATTI_JSON.exists():
        try:
            with open(ATTI_JSON, "r", encoding="utf-8") as f:
                archivio = json.load(f)
            atti_noti = {
                (a.get("numero_raw", ""), a.get("oggetto", ""))
                for a in archivio
            }
        except (json.JSONDecodeError, KeyError):
            pass

    nuovi = [
        a for a in atti
        if (a.get("numero_raw", ""), a.get("oggetto", "")) not in atti_noti
    ]
    log.info(f"Atti nuovi (non già archiviati): {len(nuovi)}")
    return nuovi


# ─────────────────────────────────────────────────────────────────────────────
# 4. DETTAGLIO ATTO + DOWNLOAD ALLEGATI
# ─────────────────────────────────────────────────────────────────────────────

def elabora_atto(atto: dict) -> dict:
    """
    Per un singolo atto:
    1. Visita la pagina di dettaglio
    2. Estrae metadati aggiuntivi e link agli allegati PDF
    3. Scarica i PDF (max MAX_ALLEGATI)
    4. Legge il testo (digitale o OCR)
    5. Genera un ID univoco e una struttura di cartelle
    """
    if not atto.get("url_dettaglio"):
        log.warning(f"Atto senza URL dettaglio: {atto.get('oggetto', '?')}")
        return atto

    log.info(f"Elaboro: {atto['oggetto'][:60]}...")

    # Prova prima con l'URL originale, poi senza il parametro pop_up
    # (Liferay pop_up a volte non include gli allegati senza sessione browser)
    urls_da_provare = [atto["url_dettaglio"]]
    url_no_popup = re.sub(r"[&?]p_p_state=pop_up", "", atto["url_dettaglio"])
    if url_no_popup != atto["url_dettaglio"]:
        urls_da_provare.append(url_no_popup)

    html_dettaglio = None
    for url_tentativo in urls_da_provare:
        try:
            html_dettaglio = _fetch(url_tentativo)
            break
        except requests.RequestException as e:
            log.warning(f"  Tentativo fallito ({url_tentativo[:80]}): {e}")

    if not html_dettaglio:
        log.error(f"Impossibile accedere al dettaglio di: {atto.get('oggetto','?')[:50]}")
        return atto

    soup = BeautifulSoup(html_dettaglio, "html.parser")

    # ── Metadati aggiuntivi dalla pagina di dettaglio ────────────────────────
    atto["numero"]    = _estrai_numero(atto.get("numero_raw", ""))
    atto["anno"]      = _estrai_anno(atto)
    atto["tipo_norm"] = _normalizza_tipo(atto.get("tipo", ""))
    atto["id_atto"]   = _genera_id(atto)

    # ── Cartella di destinazione dei PDF ────────────────────────────────────
    cartella_atto = ALLEGATI_DIR / str(atto["anno"]) / atto["id_atto"]
    cartella_atto.mkdir(parents=True, exist_ok=True)
    atto["cartella_locale"] = str(cartella_atto)

    # ── Link agli allegati PDF ───────────────────────────────────────────────
    # La pagina di dettaglio carica gli allegati via JavaScript (Liferay portlet),
    # quindi requests non li vede nell'HTML statico.
    # Usiamo direttamente l'endpoint recuperaDettaglio con l'ID atto dall'URL.
    link_pdf = _trova_link_pdf_da_endpoint(atto["url_dettaglio"], soup)
    if link_pdf:
        log.info(f"  → {len(link_pdf)} allegati PDF trovati")
    else:
        log.warning(f"  → Nessun allegato PDF trovato per {atto.get('numero','?')} — il riassunto sarà incompleto")

    atto["allegati"] = []
    testi_pdf = []

    for i, url_pdf in enumerate(link_pdf[:MAX_ALLEGATI], start=1):
        nome_file = f"allegato_{i}.pdf"
        percorso  = cartella_atto / nome_file

        ok = _scarica_pdf(url_pdf, percorso)
        if not ok:
            continue

        testo = _estrai_testo_pdf(percorso)
        testi_pdf.append(testo)

        atto["allegati"].append({
            "nome": nome_file,
            "url_originale": url_pdf,
            "percorso_locale": str(percorso),
            "caratteri": len(testo),
        })

    atto["testo_combinato"] = "\n\n---\n\n".join(testi_pdf)
    return atto


def _trova_link_pdf_da_endpoint(url_dettaglio: str, soup_fallback: BeautifulSoup) -> list[str]:
    """
    Strategia principale: usa l'endpoint Liferay recuperaDettaglio per ottenere
    la lista degli allegati senza dipendere da JavaScript.

    L'URL di dettaglio ha la forma:
      /papca/display/5473610?p_auth=XXX&p_p_state=pop_up
    L'ID atto (5473610) serve per costruire l'URL dell'endpoint resource:
      /papca-ap?p_p_id=...&p_p_lifecycle=2&p_p_resource_id=recuperaDettaglio&...&_..._id=5473610

    Se non trova nulla, fallback sulla ricerca nell'HTML statico.
    """
    # Estrai ID atto dall'URL dettaglio (numero dopo /display/)
    id_match = re.search(r"/display/(\d+)", url_dettaglio)
    if not id_match:
        log.debug("  ID atto non trovato nell'URL dettaglio, uso fallback HTML")
        return _trova_link_pdf(soup_fallback)

    id_atto = id_match.group(1)
    # Estrai p_auth dall'URL (token di sessione Liferay)
    auth_match = re.search(r"p_auth=([^&]+)", url_dettaglio)
    p_auth = auth_match.group(1) if auth_match else ""

    PORTLET = "jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"
    P = f"_{PORTLET}_"

    # Endpoint 1: recuperaDettaglio — restituisce HTML della scheda atto con link allegati
    url_endpoint = (
        f"{BASE_URL}/web/trasparenza/papca-ap"
        f"?p_p_id={PORTLET}"
        f"&p_p_lifecycle=2"
        f"&p_p_state=pop_up"
        f"&p_p_mode=view"
        f"&p_p_resource_id=recuperaDettaglio"
        f"&p_p_cacheability=cacheLevelPage"
        f"&p_auth={p_auth}"
        f"&{P}id={id_atto}"
        f"&{P}action=mostraDettaglio"
        f"&{P}fromAction=recuperaDettaglio"
    )

    try:
        resp = SESSION.get(url_endpoint, timeout=90)
        log.info(f"  Endpoint recuperaDettaglio: status={resp.status_code} len={len(resp.text)} cookie={list(SESSION.cookies.keys())}")
        if resp.status_code == 200 and len(resp.text) > 200:
            soup2 = BeautifulSoup(resp.text, "html.parser")
            links = _trova_link_pdf(soup2)
            if links:
                log.info(f"  Trovati {len(links)} PDF via endpoint recuperaDettaglio")
                return links
            log.info(f"  Risposta endpoint (primi 300 chars): {resp.text[:300]}")
        elif resp.status_code == 200 and len(resp.text) <= 200:
            log.warning(f"  Risposta vuota dall'endpoint: '{resp.text[:100]}' — cookie mancanti?")
            # Cerca anche pattern downloadAllegato direttamente nel testo
            ids_allegati = re.findall(r"[_&]id=(\d+).*?downloadAllegato|downloadAllegato.*?[_&]id=(\d+)", resp.text)
            if not ids_allegati:
                # Cerca pattern più semplice: id= nei link download
                ids_allegati = re.findall(r"downloadAllegato[^\"']*[_&]id=(\d+)", resp.text)
            urls = []
            for id_all in ids_allegati:
                id_val = id_all if isinstance(id_all, str) else (id_all[0] or id_all[1])
                if id_val:
                    url_pdf = (
                        f"{BASE_URL}/web/trasparenza/papca-ap"
                        f"?p_p_id={PORTLET}"
                        f"&p_p_lifecycle=2"
                        f"&p_p_state=pop_up"
                        f"&p_p_mode=view"
                        f"&p_p_resource_id=downloadAllegato"
                        f"&p_p_cacheability=cacheLevelPage"
                        f"&{P}downloadSigned=false"
                        f"&{P}id={id_val}"
                        f"&{P}action=mostraDettaglio"
                        f"&{P}fromAction=recuperaDettaglio"
                    )
                    if url_pdf not in urls:
                        urls.append(url_pdf)
            if urls:
                log.debug(f"  Trovati {len(urls)} PDF via ID allegato in endpoint")
                return urls
    except Exception as e:
        log.warning(f"  Endpoint recuperaDettaglio fallito: {e}")

    # Fallback: cerca nell'HTML statico già scaricato
    links = _trova_link_pdf(soup_fallback)
    if not links:
        log.debug("  Nessun PDF trovato né via endpoint né via HTML statico")
    return links


def _trova_link_pdf(soup: BeautifulSoup) -> list[str]:
    """
    Cerca i link agli allegati PDF nella pagina di dettaglio.
    JCityGov usa diversi pattern:
      - Link diretti a file .pdf
      - Endpoint Liferay: p_p_resource_id=downloadAllegato&id=XXX
      - Link con testo "Scarica" o icona PDF
    """
    url_pdf = []
    visti = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        testo = tag.get_text(strip=True).lower()

        # Pattern 1: href diretto a PDF
        if href.lower().endswith(".pdf"):
            url = href if href.startswith("http") else BASE_URL + href
            if url not in visti:
                url_pdf.append(url)
                visti.add(url)
            continue

        # Pattern 2: endpoint Liferay downloadAllegato
        if "downloadAllegato" in href or "download" in href.lower():
            url = href if href.startswith("http") else BASE_URL + href
            if url not in visti:
                url_pdf.append(url)
                visti.add(url)
            continue

        # Pattern 3: link con testo tipico degli allegati
        if any(kw in testo for kw in ["scarica", "allegato", "download", "pdf"]):
            url = href if href.startswith("http") else BASE_URL + href
            if url not in visti and url != BASE_URL:
                url_pdf.append(url)
                visti.add(url)

    return url_pdf


def _scarica_pdf(url: str, destinazione: Path) -> bool:
    """Scarica un PDF e lo salva in destinazione. Restituisce True se ok."""
    if destinazione.exists():
        log.debug(f"PDF già presente, salto: {destinazione.name}")
        return True
    try:
        resp = SESSION.get(url, timeout=60, stream=True)
        resp.raise_for_status()

        # Verifica che sia davvero un PDF
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            # Tenta comunque: a volte JCityGov non imposta correttamente il Content-Type
            pass

        with open(destinazione, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        log.info(f"  ✓ Scaricato: {destinazione.name} ({destinazione.stat().st_size // 1024} KB)")
        return True

    except Exception as e:
        log.error(f"  ✗ Errore download PDF ({url}): {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 5. LETTURA TESTO PDF (digitale + OCR automatico)
# ─────────────────────────────────────────────────────────────────────────────

def _estrai_testo_pdf(percorso: Path) -> str:
    """
    Estrae il testo da un PDF.
    Se il PDF è scansionato (< SOGLIA_OCR caratteri/pagina) attiva Tesseract OCR.
    """
    testo = ""

    # ── Tentativo 1: estrazione digitale con pdfplumber ──────────────────────
    try:
        with pdfplumber.open(percorso) as pdf:
            pagine_testo = []
            for pagina in pdf.pages:
                t = pagina.extract_text() or ""
                pagine_testo.append(t)

            testo = "\n".join(pagine_testo).strip()
            media_caratteri = len(testo) / max(len(pagine_testo), 1)
            log.debug(f"  PDF digitale: {len(testo)} char ({media_caratteri:.0f}/pagina)")

    except Exception as e:
        log.warning(f"  pdfplumber fallito su {percorso.name}: {e}")
        media_caratteri = 0

    # ── Tentativo 2: OCR se il testo è troppo scarso ─────────────────────────
    if media_caratteri < SOGLIA_OCR and OCR_DISPONIBILE:
        log.info(f"  → Attivo OCR Tesseract su {percorso.name} (media {media_caratteri:.0f} char/pag)")
        testo = _ocr_pdf(percorso)
    elif media_caratteri < SOGLIA_OCR and not OCR_DISPONIBILE:
        log.warning(
            f"  PDF probabilmente scansionato ({media_caratteri:.0f} char/pag) "
            f"ma Tesseract non disponibile."
        )

    return testo.strip()


def _ocr_pdf(percorso: Path) -> str:
    """Esegue OCR con Tesseract su un PDF scansionato (lingua italiana)."""
    try:
        immagini = convert_from_path(str(percorso), dpi=300)
        testi = []
        for img in immagini:
            t = pytesseract.image_to_string(img, lang="ita")
            testi.append(t)
        return "\n".join(testi).strip()
    except Exception as e:
        log.error(f"  Errore OCR su {percorso.name}: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# 6. RIASSUNTO CON CLAUDE API
# ─────────────────────────────────────────────────────────────────────────────

def genera_riassunto(atto: dict) -> str:
    """
    Genera un riassunto in linguaggio semplice dell'atto usando Groq (Llama 3.3 70B).
    Completamente gratuito. Usa il testo dei PDF allegati + i metadati dell'atto.
    """
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        log.warning("GROQ_API_KEY non impostata, salto riassunto.")
        return ""

    tipo    = atto.get("tipo", "Atto")
    numero  = atto.get("numero", "?")
    anno    = atto.get("anno", "?")
    oggetto = atto.get("oggetto", "")
    testo   = atto.get("testo_combinato", "")

    # Tronca il testo se troppo lungo (Llama 3.3 supporta ~32k token)
    testo_troncato = testo[:40000] if len(testo) > 40000 else testo

    prompt = f"""Sei un assistente che aiuta i cittadini del Comune di Pieve Emanuele (MI) a capire gli atti amministrativi pubblici.

Atto: {tipo} n. {numero}/{anno}
Oggetto: {oggetto}

Testo allegati:
{testo_troncato if testo_troncato else "(nessun allegato leggibile)"}

Scrivi un riassunto in italiano semplice, comprensibile a tutti i cittadini, di massimo 200 parole.

REGOLE OBBLIGATORIE:
1. Se nell'atto è presente un impegno di spesa o un importo in euro (€), DEVI citarlo esplicitamente nel riassunto con il valore esatto (es. "impegno di spesa di € 12.500,00").
2. Se è presente un fornitore o beneficiario del pagamento, citalo.
3. Spiega brevemente di cosa si tratta e perché il Comune ha preso questa decisione.
4. Usa un tono neutro e informativo. Inizia direttamente con il riassunto, senza intestazioni o prefazioni."""

    try:
        client = Groq(api_key=api_key)
        risposta = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        riassunto = risposta.choices[0].message.content.strip()
        log.info(f"  ✓ Riassunto generato ({len(riassunto)} char)")
        return riassunto
    except Exception as e:
        log.error(f"  Errore Groq API: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# 7. SALVATAGGIO JSON
# ─────────────────────────────────────────────────────────────────────────────

def archivia_wayback(atto: dict) -> str:
    """
    Invia l'URL della pagina di dettaglio dell'atto al Wayback Machine
    (Internet Archive) e restituisce l'URL della copia archiviata.

    API: POST https://web.archive.org/save/<url>
    Risposta: header 'Content-Location' con il percorso /web/YYYYMMDDHHMMSS/url
    Gratuita, nessuna autenticazione richiesta.
    """
    url = atto.get("url_dettaglio", "")
    if not url:
        return ""

    save_url = f"https://web.archive.org/save/{url}"
    try:
        resp = SESSION.post(save_url, timeout=30, allow_redirects=True)
        # L'archivio risponde con Content-Location: /web/20260628.../url
        location = resp.headers.get("Content-Location", "")
        if location:
            archived = f"https://web.archive.org{location}"
            log.info(f"  Wayback Machine: {archived[:80]}")
            return archived
        # Fallback: costruisci l'URL dalla risposta finale
        if "web.archive.org/web/" in resp.url:
            log.info(f"  Wayback Machine (redirect): {resp.url[:80]}")
            return resp.url
        log.warning(f"  Wayback Machine: risposta non attesa ({resp.status_code})")
    except Exception as e:
        log.warning(f"  Wayback Machine: errore ({e})")
    return ""


def salva_risultati(nuovi_atti: list[dict]):
    """
    Aggiorna data/atti.json (archivio completo) e
    scrive data/nuovi_atti.json (solo gli atti di questa esecuzione).
    """
    DATA_DIR.mkdir(exist_ok=True)

    # Archivio completo
    archivio = []
    if ATTI_JSON.exists():
        try:
            with open(ATTI_JSON, "r", encoding="utf-8") as f:
                archivio = json.load(f)
        except json.JSONDecodeError:
            archivio = []

    # Aggiunge i nuovi atti in testa
    archivio = nuovi_atti + archivio

    with open(ATTI_JSON, "w", encoding="utf-8") as f:
        json.dump(archivio, f, ensure_ascii=False, indent=2)
    log.info(f"Archivio aggiornato: {len(archivio)} atti totali in {ATTI_JSON}")

    # Solo i nuovi
    with open(NUOVI_ATTI_JSON, "w", encoding="utf-8") as f:
        json.dump(nuovi_atti, f, ensure_ascii=False, indent=2)
    log.info(f"Nuovi atti scritti in {NUOVI_ATTI_JSON}: {len(nuovi_atti)}")


# ─────────────────────────────────────────────────────────────────────────────
# 8. FUNZIONI HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _estrai_numero(numero_raw: str) -> str:
    """Estrae solo il numero dal campo 'Anno/Numero' (es. '2025/042' → '042')."""
    parti = re.split(r"[/\-]", numero_raw.strip())
    if len(parti) >= 2:
        return parti[-1].strip().zfill(3)
    return numero_raw.strip()


def _estrai_anno(atto: dict) -> int:
    """Estrae l'anno dall'atto (dalla data o dal numero_raw)."""
    # Prova dalla data di inizio
    data = atto.get("data_inizio") or atto.get("data_fine") or ""
    match = re.search(r"\b(20\d{2})\b", data)
    if match:
        return int(match.group(1))

    # Prova dal numero_raw
    numero_raw = atto.get("numero_raw", "")
    match = re.search(r"\b(20\d{2})\b", numero_raw)
    if match:
        return int(match.group(1))

    return date.today().year


def _normalizza_tipo(tipo: str) -> str:
    """Normalizza il tipo atto in una stringa URL-friendly."""
    tipo_lower = tipo.lower()
    if "delibera" in tipo_lower:
        return "delibera"
    if "determinazione" in tipo_lower or "determina" in tipo_lower:
        return "determinazione"
    if "ordinanza" in tipo_lower:
        return "ordinanza"
    if "avviso" in tipo_lower:
        return "avviso"
    if "bando" in tipo_lower:
        return "bando"
    if "appalto" in tipo_lower or "gara" in tipo_lower:
        return "appalto"
    if "variazione" in tipo_lower:
        return "variazione-bilancio"
    return re.sub(r"[^a-z0-9]", "-", tipo_lower).strip("-")


def _genera_id(atto: dict) -> str:
    """
    Genera un ID univoco e leggibile per l'atto.
    Formato: tipo-numero-anno (es. 'delibera-042-2025')
    Usato come nome cartella e come identifier Internet Archive.
    """
    tipo   = atto.get("tipo_norm", "atto")
    numero = atto.get("numero", "000")
    anno   = atto.get("anno", date.today().year)

    base = f"{tipo}-{numero}-{anno}"
    # Sanifica per filesystem e URL
    base = re.sub(r"[^a-z0-9\-]", "", base.lower())
    return base


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--forza-riassunti", action="store_true",
                        help="Rigenera riassunti e PDF per tutti gli atti esistenti")
    args, _ = parser.parse_known_args()

    log.info("=" * 60)
    log.info("ALBO PRETORIO — Comune di Pieve Emanuele")
    log.info(f"Esecuzione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 0. Inizializza sessione con cookie Liferay (necessari per endpoint allegati)
    _init_sessione()

    # 1. Scraping lista completa
    tutti_atti = scrape_lista_atti()
    if not tutti_atti:
        log.warning("Nessun atto trovato. Verifica l'URL o la struttura della pagina.")
        salva_risultati([])
        return

    # 2. Rimuovi duplicati introdotti dal portale (stesso atto, due righe)
    tutti_atti = deduplica_lista(tutti_atti)

    # 3. Filtra atti rilevanti
    atti_rilevanti = applica_filtri(tutti_atti)

    # 4a. Rigenera riassunti mancanti (o tutti se --forza-riassunti)
    atti_json_path = Path("data/atti.json")
    if atti_json_path.exists():
        atti_salvati = json.loads(atti_json_path.read_text(encoding="utf-8"))
        if args.forza_riassunti:
            # Svuota testo e allegati così li riscarica dal portale
            for a in atti_salvati:
                a["riassunto"] = ""
                a["testo_combinato"] = ""
                a["allegati"] = []
            log.info(f"--forza-riassunti: rigenerazione forzata per {len(atti_salvati)} atti")
        senza_riassunto = [a for a in atti_salvati if not a.get("riassunto")]
        if senza_riassunto:
            log.info(f"Rigenerazione riassunti per {len(senza_riassunto)} atti esistenti...")
            for a in senza_riassunto:
                log.info(f"  Riassunto: {a.get('tipo','?')} {a.get('numero','?')}")
                # Se testo_combinato è vuoto, scarica i PDF (elabora_atto)
                if not a.get("testo_combinato"):
                    # Prima prova PDF già su disco
                    cartella = a.get("cartella_locale", "")
                    if cartella and Path(cartella).exists():
                        testi = []
                        for pdf_path in sorted(Path(cartella).glob("*.pdf")):
                            t = _estrai_testo_pdf(pdf_path)
                            if t:
                                testi.append(t)
                        if testi:
                            a["testo_combinato"] = "\n\n---\n\n".join(testi)
                            log.info(f"  → Testo da PDF su disco: {len(a['testo_combinato'])} chars")
                    # Se ancora vuoto e ha URL dettaglio, scarica i PDF dal portale
                    if not a.get("testo_combinato") and a.get("url_dettaglio"):
                        log.info(f"  → Scarico PDF dal portale...")
                        a = elabora_atto(a)
                        if a.get("testo_combinato"):
                            log.info(f"  → Testo estratto: {len(a['testo_combinato'])} chars")
                        else:
                            log.warning(f"  → Nessun testo estratto dai PDF")
                a["riassunto"] = genera_riassunto(a)
                time.sleep(1)
            atti_json_path.write_text(
                json.dumps(atti_salvati, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            log.info("Riassunti rigenerati e salvati.")

    # 4b. Identifica solo i nuovi
    nuovi_atti = filtra_nuovi(atti_rilevanti)

    if not nuovi_atti:
        log.info("Nessun atto nuovo oggi. Tutto aggiornato.")
        salva_risultati([])
        return

    # 5. Per ogni atto nuovo: dettaglio + PDF + OCR + riassunto
    atti_elaborati = []
    for i, atto in enumerate(nuovi_atti, start=1):
        log.info(f"[{i}/{len(nuovi_atti)}] {atto.get('tipo', '?')} — {atto.get('oggetto', '?')[:50]}")

        atto = elabora_atto(atto)
        atto["riassunto"] = genera_riassunto(atto)
        atto["url_archivio"] = archivia_wayback(atto)
        atto["data_elaborazione"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        atti_elaborati.append(atto)
        time.sleep(1)  # pausa tra gli atti per non sovraccaricare il server

    # 5. Salva i risultati
    salva_risultati(atti_elaborati)

    log.info("=" * 60)
    log.info(f"Fine. {len(atti_elaborati)} atti nuovi elaborati.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
