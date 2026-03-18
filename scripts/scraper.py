"""
scraper.py — Albo Pretorio Comune di Pieve Emanuele
Piattaforma: JCityGov di Maggioli (Liferay)
Eseguito da GitHub Actions ogni giorno alle 08:00 (cron 0 6 * * *)
"""

import os
import re
import json
import time
import hashlib
import logging
import requests
import anthropic
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
ALBO_URL = f"{BASE_URL}/web/trasparenza/dettaglio-albo-pretorio"

# ID portlet Liferay dell'albo pubblicazioni
PORTLET_ID = "jcitygovalbopubblicazioni_WAR_jcitygovalbiportlet"

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

# ── Sessione HTTP ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (compatible; AlboPretorioBot/1.0; "
        "+https://github.com/TUO_USERNAME/albo-pretorio)"
    )
})


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
            resp = SESSION.get(url_corrente, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error(f"Errore HTTP sulla pagina {pagina}: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Cerca la tabella degli atti (prima tabella nella pagina)
        tabella = soup.find("table")
        if not tabella:
            log.warning(f"Nessuna tabella trovata a pagina {pagina}. Fine elenco.")
            # DEBUG: stampa i primi 3000 caratteri dell'HTML per capire la struttura
            log.warning("=== DEBUG HTML (prime 3000 caratteri) ===")
            log.warning(resp.text[:3000])
            log.warning("=== TUTTI I TAG DIV CON CLASSE ===")
            for div in soup.find_all("div", class_=True)[:20]:
                log.warning(f"  <div class='{' '.join(div.get('class', []))}'>")
            log.warning("=== TUTTI I TAG TABLE ===")
            for t in soup.find_all(["table", "tbody", "tr"])[:10]:
                log.warning(f"  <{t.name} class='{t.get('class', '')}'>")
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

def filtra_nuovi(atti: list[dict]) -> list[dict]:
    """
    Restituisce solo gli atti non ancora presenti in data/atti.json.
    Usa l'URL del dettaglio come identificatore univoco.
    """
    atti_noti = set()
    if ATTI_JSON.exists():
        try:
            with open(ATTI_JSON, "r", encoding="utf-8") as f:
                archivio = json.load(f)
            atti_noti = {a["url_dettaglio"] for a in archivio if a.get("url_dettaglio")}
        except (json.JSONDecodeError, KeyError):
            pass

    nuovi = [a for a in atti if a.get("url_dettaglio") not in atti_noti]
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

    try:
        resp = SESSION.get(atto["url_dettaglio"], timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Errore accesso dettaglio: {e}")
        return atto

    soup = BeautifulSoup(resp.text, "html.parser")

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
    link_pdf = _trova_link_pdf(soup)
    log.info(f"  → {len(link_pdf)} allegati PDF trovati")

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
    Genera un riassunto in linguaggio semplice dell'atto usando Claude Haiku.
    Usa il testo dei PDF allegati + i metadati dell'atto.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY non impostata, salto riassunto.")
        return ""

    tipo    = atto.get("tipo", "Atto")
    numero  = atto.get("numero", "?")
    anno    = atto.get("anno", "?")
    oggetto = atto.get("oggetto", "")
    testo   = atto.get("testo_combinato", "")

    # Tronca il testo se troppo lungo (limite ~50.000 caratteri per Haiku)
    testo_troncato = testo[:50000] if len(testo) > 50000 else testo

    prompt = f"""Sei un assistente che aiuta i cittadini del Comune di Pieve Emanuele (MI) a capire gli atti amministrativi pubblici.

Atto: {tipo} n. {numero}/{anno}
Oggetto: {oggetto}

Testo allegati:
{testo_troncato if testo_troncato else "(nessun allegato leggibile)"}

Scrivi un riassunto in italiano semplice, comprensibile a tutti i cittadini, di massimo 150 parole.
Spiega: di cosa si tratta, cosa cambia per i cittadini, quali importi o decisioni rilevanti sono presenti.
Usa un tono neutro e informativo. Inizia direttamente con il riassunto, senza intestazioni."""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        messaggio = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        riassunto = messaggio.content[0].text.strip()
        log.info(f"  ✓ Riassunto generato ({len(riassunto)} char)")
        return riassunto
    except Exception as e:
        log.error(f"  Errore Claude API: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# 7. SALVATAGGIO JSON
# ─────────────────────────────────────────────────────────────────────────────

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
    log.info("=" * 60)
    log.info("ALBO PRETORIO — Comune di Pieve Emanuele")
    log.info(f"Esecuzione: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 1. Scraping lista completa
    tutti_atti = scrape_lista_atti()
    if not tutti_atti:
        log.warning("Nessun atto trovato. Verifica l'URL o la struttura della pagina.")
        salva_risultati([])
        return

    # 2. Filtra atti rilevanti
    atti_rilevanti = applica_filtri(tutti_atti)

    # 3. Identifica solo i nuovi
    nuovi_atti = filtra_nuovi(atti_rilevanti)

    if not nuovi_atti:
        log.info("Nessun atto nuovo oggi. Tutto aggiornato.")
        salva_risultati([])
        return

    # 4. Per ogni atto nuovo: dettaglio + PDF + OCR + riassunto
    atti_elaborati = []
    for i, atto in enumerate(nuovi_atti, start=1):
        log.info(f"[{i}/{len(nuovi_atti)}] {atto.get('tipo', '?')} — {atto.get('oggetto', '?')[:50]}")

        atto = elabora_atto(atto)
        atto["riassunto"] = genera_riassunto(atto)
        atto["data_elaborazione"] = datetime.now().strftime("%Y-%m-%d")

        atti_elaborati.append(atto)
        time.sleep(2)  # pausa tra gli atti per non sovraccaricare il server

    # 5. Salva i risultati
    salva_risultati(atti_elaborati)

    log.info("=" * 60)
    log.info(f"Fine. {len(atti_elaborati)} atti nuovi elaborati.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
