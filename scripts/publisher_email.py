"""
publisher_email.py — Invia la digest dei nuovi atti via Buttondown.
Viene eseguito dopo scraper.py nel workflow GitHub Actions.

Secrets necessari:
  BUTTONDOWN_API_KEY — da buttondown.com → Settings → API
"""

import os
import json
import logging
import requests
from pathlib import Path
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

NUOVI_ATTI_JSON   = Path("data/nuovi_atti.json")
BUTTONDOWN_API    = "https://api.buttondown.email/v1/emails"
SITO_URL          = "https://nt0wers84.github.io/albo-pretorio/"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _tipo_display(atto: dict) -> str:
    tipo_raw = atto.get("tipo", "Atto")
    if "/" in tipo_raw:
        tipo_raw = tipo_raw.split("/")[-1].strip()
    return tipo_raw.title()


def _esc(testo: str) -> str:
    """Escape minimale per HTML."""
    return (testo or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── HTML email ────────────────────────────────────────────────────────────────

def _card_atto(atto: dict) -> str:
    tipo      = _esc(_tipo_display(atto))
    numero    = _esc(atto.get("numero_raw", "?"))
    oggetto   = _esc(atto.get("oggetto", ""))
    riassunto = _esc(atto.get("riassunto", ""))
    url       = atto.get("url_dettaglio", SITO_URL)

    riassunto_html = (
        f'<p style="font-size:13px;color:#636158;line-height:1.75;margin:0 0 14px">'
        f'{riassunto}</p>'
    ) if riassunto else ""

    return (
        f'<div style="background:#fff;border:0.5px solid rgba(0,0,0,.08);'
        f'border-radius:12px;padding:18px 20px;margin-bottom:16px">'
        f'  <div style="font-size:11px;font-weight:600;color:#123785;'
        f'text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px">'
        f'{tipo} · n.&nbsp;{numero}</div>'
        f'  <h2 style="font-size:14px;font-weight:500;color:#141412;'
        f'line-height:1.55;margin:0 0 10px">{oggetto}</h2>'
        f'  {riassunto_html}'
        f'  <a href="{url}" style="font-size:12px;color:#123785;'
        f'text-decoration:none;font-weight:500">Leggi l\'atto completo →</a>'
        f'</div>'
    )


def costruisci_email(atti: list[dict], oggi: str) -> tuple[str, str]:
    """Restituisce (subject, body_html)."""
    n = len(atti)
    intro = (
        f"Oggi {'è stato pubblicato' if n == 1 else 'sono stati pubblicati'} "
        f"<strong>{n} {'atto' if n == 1 else 'atti'} nuovi</strong> "
        f"sull'Albo Pretorio del Comune di Pieve Emanuele."
    )

    cards = "\n".join(_card_atto(a) for a in atti)

    body = f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  color:#141412;background:#F2F1ED;margin:0;padding:0">
  <div style="max-width:600px;margin:0 auto;padding:36px 20px 48px">

    <!-- Header -->
    <div style="text-align:center;margin-bottom:36px">
      <p style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;
        color:#888;margin:0 0 6px">Comune di Pieve Emanuele</p>
      <h1 style="font-size:22px;font-weight:600;color:#123785;margin:0 0 4px">
        Albo in chiaro</h1>
      <p style="font-size:13px;color:#636158;margin:0">{_esc(oggi)}</p>
    </div>

    <!-- Intro -->
    <p style="font-size:14px;color:#444;line-height:1.7;margin:0 0 28px">
      {intro}
    </p>

    <!-- Atti -->
    {cards}

    <!-- Footer -->
    <div style="margin-top:40px;padding-top:20px;
      border-top:1px solid rgba(0,0,0,.08);text-align:center">
      <p style="font-size:11px;color:#888;line-height:1.7;margin:0">
        <a href="{SITO_URL}" style="color:#123785;text-decoration:none">
          Visita il sito</a>
        &nbsp;·&nbsp;
        Progetto civico indipendente, non affiliato al Comune di Pieve Emanuele
      </p>
    </div>

  </div>
</body>
</html>"""

    subject = f"Albo Pretorio PE — {n} {'atto' if n == 1 else 'atti'} nuovi · {oggi}"
    return subject, body


# ── Invio Buttondown ──────────────────────────────────────────────────────────

def invia_email(api_key: str, subject: str, body: str) -> bool:
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "subject": subject,
        "body": body,
        "email_type": "public",
        "status": "about_to_send",
    }
    try:
        resp = requests.post(BUTTONDOWN_API, headers=headers, json=payload, timeout=30)
        if resp.status_code in (200, 201):
            log.info(f"Email inviata con successo (status {resp.status_code})")
            return True
        else:
            log.error(f"Buttondown API error {resp.status_code}: {resp.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Errore invio email: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("BUTTONDOWN_API_KEY")
    if not api_key:
        log.warning("BUTTONDOWN_API_KEY non impostata. Salto.")
        return

    if not NUOVI_ATTI_JSON.exists():
        log.warning("Nessun file nuovi_atti.json trovato.")
        return

    with open(NUOVI_ATTI_JSON, "r", encoding="utf-8") as f:
        atti = json.load(f)

    if not atti:
        log.info("Nessun atto nuovo — nessuna email da inviare.")
        return

    # Ordina per numero progressivo (stesso criterio del publisher Telegram)
    def _sort_key(a: dict) -> int:
        raw = (a.get("numero_raw") or "").split("/")[-1]
        try:
            return int(raw)
        except ValueError:
            return 0

    atti = sorted(atti, key=_sort_key)

    oggi = date.today().strftime("%d/%m/%Y")
    log.info(f"Invio email con {len(atti)} atti nuovi...")

    subject, body = costruisci_email(atti, oggi)
    ok = invia_email(api_key, subject, body)

    if ok:
        log.info("Newsletter inviata.")
    else:
        log.error("Invio newsletter fallito.")


if __name__ == "__main__":
    main()
