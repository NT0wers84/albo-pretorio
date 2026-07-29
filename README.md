# Albo Pretorio — Pieve Emanuele

Monitoraggio civico indipendente dell'Albo Pretorio del Comune di Pieve Emanuele (MI).

Ogni giorno un automatismo scarica gli atti pubblicati (delibere, determinazioni,
ordinanze, bandi…), li riassume con l'AI (Groq / Llama), pubblica le novità su
Telegram e aggiorna un sito statico consultabile da chiunque.

Sito: https://nt0wers84.github.io/albo-pretorio/  
Progetto gemello: https://nt0wers84.github.io/bilanciopertutti/

## Architettura

- `scripts/scraper.py` — scraping giornaliero dell'albo (piattaforma JCityGov/Liferay),
  download PDF, OCR con Tesseract, riassunto AI via Groq
- `scripts/publisher_telegram.py` — pubblica i nuovi atti sul canale Telegram,
  ordinati per numero progressivo crescente
- `scripts/publisher_email.py` — invia la digest HTML dei nuovi atti via Buttondown
  (newsletter gratuita, attivata solo se `BUTTONDOWN_API_KEY` è impostata)
- `scripts/aggiorna_standalone.py` — inietta i dati aggiornati nel bundle HTML del sito
  (patch idempotenti sul template JSON)
- `scripts/genera_rss.py` — genera `docs/feed.xml` con gli ultimi 30 giorni di atti
- `scripts/mostra_risultati.py` — log di riepilogo nel workflow GitHub Actions
- `data/atti.json` — archivio flat (unica fonte di verità)
- `docs/` — sito statico servito da GitHub Pages

## Setup (una tantum)

1. **Secrets** (Settings → Secrets and variables → Actions):
   - `GROQ_API_KEY` — da https://console.groq.com (piano gratuito disponibile)
   - `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHANNEL_ID` — opzionali
   - `BUTTONDOWN_API_KEY` — da https://buttondown.com/settings (opzionale, per newsletter email)
2. **GitHub Pages**: Settings → Pages → Source: branch `main`, cartella `/docs`

Il workflow parte automaticamente ogni giorno alle 15:00 UTC (~17:00 ora italiana,
considerando il tipico ritardo di ~2h dei runner condivisi GitHub).

## Note

- I PDF non vengono archiviati nel repository: ogni scheda linka l'atto originale
  sul portale comunale.
- L'estrazione AI può contenere errori: fa fede sempre l'atto originale.
- Feed RSS disponibile su `/feed.xml` con titolo, riassunto e link degli ultimi 30 giorni.
- Il sito mostra per default gli atti degli ultimi 2 giorni; la barra di ricerca
  interroga l'intero archivio.
