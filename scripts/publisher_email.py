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
        f'<p style="font-size:13px;color:#555350;line-height:1.8;margin:0 0 16px">'
        f'{riassunto}</p>'
    ) if riassunto else ""

    return (
        # Card con bordo sinistro blu e sfondo bianco
        f'<div style="background:#ffffff;border-radius:12px;margin-bottom:16px;'
        f'border-left:3px solid #1B4FCA;padding:20px 22px;'
        f'border-top:1px solid rgba(0,0,0,.07);border-right:1px solid rgba(0,0,0,.07);'
        f'border-bottom:1px solid rgba(0,0,0,.07)">'
        # Chip tipo
        f'<div style="display:inline-block;background:#EEF2FF;color:#1B4FCA;'
        f'font-size:10px;font-weight:700;padding:3px 10px;border-radius:100px;'
        f'letter-spacing:.08em;text-transform:uppercase;margin-bottom:12px">'
        f'{tipo}&nbsp;·&nbsp;n.&nbsp;{numero}</div>'
        # Titolo
        f'<p style="font-size:14px;font-weight:600;color:#141412;'
        f'line-height:1.55;margin:0 0 12px">{oggetto}</p>'
        # Riassunto
        f'{riassunto_html}'
        # Link
        f'<a href="{url}" style="font-size:12px;color:#1B4FCA;font-weight:600;'
        f'text-decoration:none">Leggi l\'atto completo&nbsp;→</a>'
        f'</div>'
    )


def costruisci_email(atti: list[dict], oggi: str) -> tuple[str, str]:
    """Restituisce (subject, body_html)."""
    n = len(atti)
    # Grammatica corretta: "1 atto nuovo" / "N atti nuovi"
    if n == 1:
        intro = "Oggi è stato pubblicato <strong>1 atto nuovo</strong> sull'Albo Pretorio del Comune di Pieve Emanuele."
    else:
        intro = f"Oggi sono stati pubblicati <strong>{n} atti nuovi</strong> sull'Albo Pretorio del Comune di Pieve Emanuele."

    cards = "\n".join(_card_atto(a) for a in atti)

    body = f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;
  color:#141412;background:#E8E6E1;margin:0;padding:0">

  <!-- Header blu -->
  <div style="background:#123785;padding:36px 24px 32px;text-align:center">
    <p style="font-size:10px;letter-spacing:.14em;text-transform:uppercase;
      color:rgba(255,255,255,.65);margin:0 0 10px">Comune di Pieve Emanuele</p>
    <h1 style="font-size:26px;font-weight:700;color:#ffffff;margin:0 0 8px;
      letter-spacing:-.02em">Albo in chiaro</h1>
    <p style="font-size:13px;color:rgba(255,255,255,.75);margin:0">{_esc(oggi)}</p>
  </div>

  <!-- Corpo -->
  <div style="max-width:600px;margin:0 auto;padding:32px 20px 48px">

    <!-- Intro -->
    <p style="font-size:14px;color:#444;line-height:1.75;margin:0 0 28px">
      {intro}
    </p>

    <!-- Atti -->
    {cards}

    <!-- Footer -->
    <div style="margin-top:40px;padding-top:20px;
      border-top:1px solid rgba(0,0,0,.10);text-align:center">
      <p style="font-size:11px;color:#888;line-height:1.8;margin:0">
        <a href="{SITO_URL}" style="color:#1B4FCA;text-decoration:none;font-weight:500">
          Visita il sito</a>
        &nbsp;·&nbsp;
        Progetto civico indipendente, non affiliato al Comune di Pieve Emanuele
      </p>
    </div>

  </div>
</body>
</html>"""

    subject = f"Albo in chiaro — {n} {'atto' if n == 1 else 'atti'} {'nuovo' if n == 1 else 'nuovi'} · {oggi}"
    return subject, body


# ── Invio Buttondown ──────────────────────────────────────────────────────────

def invia_email(api_key: str, subject: str, body: str) -> bool:
    """
    Invia via Buttondown in due step:
    1. Crea come draft  → POST /v1/emails
    2. Mette in coda   → PATCH /v1/emails/{id}  status: about_to_send
    """
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json",
    }

    # Step 1 — crea draft
    payload = {
        "subject": subject,
        "body": body,
        "email_type": "public",
        "status": "draft",
    }
    try:
        resp = requests.post(BUTTONDOWN_API, headers=headers, json=payload, timeout=30)
        if resp.status_code not in (200, 201):
            log.error(f"Buttondown crea draft error {resp.status_code}: {resp.text[:300]}")
            return False
        email_id = resp.json().get("id")
        if not email_id:
            log.error("Buttondown: ID email non trovato nella risposta")
            return False
        log.info(f"Draft creato: {email_id}")
    except Exception as e:
        log.error(f"Errore creazione draft: {e}")
        return False

    # Step 2 — metti in coda di invio
    try:
        patch_url = f"{BUTTONDOWN_API}/{email_id}"
        resp2 = requests.patch(patch_url, headers=headers,
                               json={"status": "about_to_send"}, timeout=30)
        if resp2.status_code in (200, 201):
            log.info(f"Email in coda di invio (status {resp2.status_code})")
            return True
        else:
            log.error(f"Buttondown invio error {resp2.status_code}: {resp2.text[:300]}")
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
