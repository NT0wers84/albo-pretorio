"""
publisher_telegram.py — Pubblica gli atti nuovi sul canale Telegram.
Viene eseguito dopo scraper.py nel workflow GitHub Actions.

Secrets necessari:
  TELEGRAM_BOT_TOKEN  — token del bot (da @BotFather)
  TELEGRAM_CHANNEL_ID — username canale con @ (es. @albopretoriopieve)
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

NUOVI_ATTI_JSON = Path("data/nuovi_atti.json")

# Emoji per tipo atto
EMOJI_TIPO = {
    "delibera":         "🏛️",
    "determinazione":   "📋",
    "ordinanza":        "⚠️",
    "avviso":           "📢",
    "bando":            "📣",
    "appalto":          "🔨",
    "variazione-bilancio": "💰",
}


def emoji_per_tipo(tipo_norm: str) -> str:
    for chiave, em in EMOJI_TIPO.items():
        if chiave in (tipo_norm or "").lower():
            return em
    return "📄"


def formatta_messaggio_intro(atti: list[dict], oggi: str) -> str:
    """Messaggio di apertura con riepilogo del giorno."""
    n = len(atti)
    return (
        f"🏙️ *Albo Pretorio — Pieve Emanuele*\n"
        f"📅 {oggi}\n\n"
        f"Oggi {'è stato pubblicato' if n == 1 else 'sono stati pubblicati'} "
        f"*{n} {'atto' if n == 1 else 'atti'}* nuovi\\."
    )


def formatta_atto(atto: dict, indice: int, totale: int) -> str:
    """Formatta un singolo atto in Markdown Telegram (MarkdownV2)."""
    tipo_norm = atto.get("tipo_norm", "atto")
    em = emoji_per_tipo(tipo_norm)

    tipo_raw = atto.get("tipo", "Atto")
    # Prendi solo la parte dopo "/" se presente (es. "ATTI AMMINISTRATIVI/DELIBERA DI GIUNTA" → "DELIBERA DI GIUNTA")
    if "/" in tipo_raw:
        tipo_raw = tipo_raw.split("/")[-1].strip()
    tipo_display = tipo_raw.title()

    numero = atto.get("numero_raw", "?")
    oggetto = atto.get("oggetto", "")[:300]
    riassunto = atto.get("riassunto", "")  # nessun troncamento: max 200 parole dal prompt
    url = atto.get("url_dettaglio", "")

    # Escape caratteri speciali per MarkdownV2
    def esc(testo: str) -> str:
        speciali = r"\_*[]()~`>#+-=|{}.!"
        return "".join(f"\\{c}" if c in speciali else c for c in testo)

    parti = [
        f"{em} *{esc(tipo_display)}* \\— n\\. {esc(numero)}",
        f"_{esc(oggetto)}_",
    ]

    if riassunto:
        parti.append(f"\n{esc(riassunto)}")

    if url:
        parti.append(f"\n[🔗 Leggi l'atto completo]({url})")

    return "\n".join(parti)


def invia_messaggio(token: str, chat_id: str, testo: str) -> bool:
    """Invia un messaggio al canale Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": testo,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=30)
        data = resp.json()
        if data.get("ok"):
            return True
        else:
            log.error(f"Telegram API error: {data.get('description', 'unknown')}")
            # Fallback: invia come testo semplice
            payload["parse_mode"] = "HTML"
            payload["text"] = testo.replace("\\", "").replace("*", "<b>").replace("_", "<i>")
            resp2 = requests.post(url, json=payload, timeout=30)
            return resp2.json().get("ok", False)
    except Exception as e:
        log.error(f"Errore invio Telegram: {e}")
        return False


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    channel = os.environ.get("TELEGRAM_CHANNEL_ID")

    if not token or not channel:
        log.warning("TELEGRAM_BOT_TOKEN o TELEGRAM_CHANNEL_ID non impostati. Salto.")
        return

    if not NUOVI_ATTI_JSON.exists():
        log.warning("Nessun file nuovi_atti.json trovato.")
        return

    with open(NUOVI_ATTI_JSON, "r", encoding="utf-8") as f:
        atti = json.load(f)

    if not atti:
        log.info("Nessun atto nuovo da pubblicare su Telegram.")
        return

    oggi = date.today().strftime("%d/%m/%Y")
    log.info(f"Pubblico {len(atti)} atti su Telegram ({channel})")

    # 1. Messaggio introduttivo
    intro = formatta_messaggio_intro(atti, oggi)
    ok = invia_messaggio(token, channel, intro)
    log.info(f"  Intro: {'✓' if ok else '✗'}")

    # 2. Un messaggio per ogni atto
    for i, atto in enumerate(atti, start=1):
        testo = formatta_atto(atto, i, len(atti))
        ok = invia_messaggio(token, channel, testo)
        log.info(f"  [{i}/{len(atti)}] {atto.get('oggetto','?')[:50]}: {'✓' if ok else '✗'}")

    log.info("Pubblicazione Telegram completata.")


if __name__ == "__main__":
    main()
