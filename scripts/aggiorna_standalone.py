"""
aggiorna_standalone.py — Inietta gli atti aggiornati nel file standalone HTML.

Il file 'docs/index.html' è un bundle autocontenuto generato da Claude Design.
I dati degli atti sono embedded come variabile JS ALL_ATTI dentro il tag
<script type="__bundler/template"> (stringa JSON).

Le patch UI vengono applicate direttamente sul testo raw del file
(senza decode/re-encode) cercando le sequenze JSON-encodificate esatte.

Viene chiamato dal workflow GitHub Actions dopo scraper.py.
"""

import json
import re
from pathlib import Path
from datetime import datetime

ATTI_JSON  = Path("data/atti.json")
STANDALONE = Path("docs/index.html")


# ── Sequenze raw nel file (JSON-encodificate) da patchare ───────────────────
#
# Nota: nel file raw le virgolette doppie sono \\" e i tag </x> sono </x>
#
# Le sequenze raw sono estratte direttamente dal file (JSON-encoded).
# Nel file: \" → \\" in Python, </x> → /x nei tag JSON, \n → \\n

# Helper per costruire le costanti senza conflitti di escape
_Q  = '\\"'       # virgoletta escapata come nel file JSON
_SF = '<\\u002F'  # </ nei tag chiusi

# PATCH 1 — <p> del riassunto: aggiunge "leggi tutto" espandibile
# PATCH1_OLD copre due possibili stati del file:
# (a) file originale Claude Design (p semplice)
# (b) file dopo patch precedente con span nascosti
PATCH1_OLD_A = (
    f'<p style={_Q}font-size:12px;color:#636158;line-height:1.7;margin-bottom:10px{_Q}>'
    f'{{{{ item.riassuntoShort }}}}{_SF}p>'
)
PATCH1_OLD_B = (
    f'<p style={_Q}font-size:12px;color:#636158;line-height:1.7;margin-bottom:10px;margin-top:0{_Q}>'
    f'<span class={_Q}rias-short{_Q}>{{{{ item.riassuntoShort }}}}{_SF}span>'
    f'<span class={_Q}rias-full{_Q} style={_Q}display:none{_Q}>{{{{ item.riassuntoFull }}}}{_SF}span>'
    f'<button class={_Q}rias-toggle{_Q} '
    f'style={_Q}display:{{{{ item.hasTruncation }}}};cursor:pointer;color:#1B4FCA;font-size:11px;'
    f'font-weight:500;margin-left:4px;background:none;border:none;padding:0;font-family:inherit{_Q}'
    f'>leggi tutto{_SF}button>'
)
# Variante con item.riassunto (senza Full) + onclick inline — presente nel commit b3ce29a
# Usa un pattern regex per trovare qualunque variante del bottone con onclick
PATCH1_OLD_C = None  # gestita via regex nella funzione
PATCH1_OLD = PATCH1_OLD_A  # default per file originale

# onclick chiama una funzione globale — nessuna stringa da escapare nell'attributo HTML
_ONCLICK = "riasToggle(this)"

PATCH1_NEW = (
    # Wrapper con data-* che porta i dati completi — letti dal modale JS
    f'<div class={_Q}rias-wrap{_Q} '
    f'data-rias=1 '
    f'data-tipo={_Q}{{{{ item.tipo }}}}{_Q} '
    f'data-oggetto={_Q}{{{{ item.oggettoFull }}}}{_Q} '
    f'data-testo={_Q}{{{{ item.riassuntoFull }}}}{_Q} '
    f'data-data={_Q}{{{{ item.data }}}}{_Q} '
    f'data-numero={_Q}{{{{ item.numero }}}}{_Q} '
    f'data-url={_Q}{{{{ item.url }}}}{_Q}>'
    f'<p style={_Q}font-size:12px;color:#636158;line-height:1.7;margin-bottom:6px;margin-top:0{_Q}>'
    f'{{{{ item.riassuntoShort }}}}'
    f'{_SF}p>'
    f'<button class={_Q}rias-toggle{_Q} '
    f'style={_Q}display:{{{{ item.hasTruncation }}}};cursor:pointer;color:#1B4FCA;font-size:11px;'
    f'font-weight:500;background:none;border:none;padding:0;font-family:inherit;margin-bottom:8px{_Q}'
    f'>leggi tutto{_SF}button>'
    f'{_SF}div>'
)

# PATCH 2 — link footer card: aggiunge icona archivio
PATCH2_OLD = (
    f'<a href={_Q}{{{{ item.url }}}}{_Q} target={_Q}_blank{_Q} rel={_Q}noopener{_Q} '
    f'style={_Q}font-size:11px;color:#1B4FCA;text-decoration:none;display:inline-flex;'
    f'align-items:center;gap:3px;font-weight:500{_Q}>Leggi '
    f'<i class={_Q}ti ti-arrow-right{_Q} style={_Q}font-size:11px{_Q}>{_SF}i>{_SF}a>'
)
PATCH2_NEW = (
    f'<div style={_Q}display:flex;gap:8px;align-items:center{_Q}>'
    f'<a href={_Q}{{{{ item.url }}}}{_Q} target={_Q}_blank{_Q} rel={_Q}noopener{_Q} '
    f'style={_Q}font-size:11px;color:#1B4FCA;text-decoration:none;display:inline-flex;'
    f'align-items:center;gap:3px;font-weight:500{_Q}>Leggi '
    f'<i class={_Q}ti ti-arrow-right{_Q} style={_Q}font-size:11px{_Q}>{_SF}i>{_SF}a>'
    f'<sc-if value={_Q}{{{{ item.hasArchivio }}}}{_Q} hint-placeholder-val={_Q}{{{{ false }}}}{_Q}>'
    f'<a href={_Q}{{{{ item.url_archivio }}}}{_Q} target={_Q}_blank{_Q} rel={_Q}noopener{_Q} '
    f'style={_Q}font-size:11px;color:#888;text-decoration:none;display:inline-flex;'
    f'align-items:center;gap:3px{_Q} title={_Q}Copia permanente (Wayback Machine){_Q}>'
    f'<i class={_Q}ti ti-archive{_Q} style={_Q}font-size:11px{_Q}>{_SF}i>{_SF}a>'
    f'{_SF}sc-if>'
    f'{_SF}div>'
)

# PATCH 4 — Inietta modale overlay + event listener nel <head> del template
# NOTA: tutti gli apostrofi nel JS devono essere \\u0027 (non letterali)
# perché il raw è una stringa JSON e ' spezzerebbe il parser.
# Il check "già presente" usa 'rias-ov' (id univoco del modale).
_Q4 = "\\u0027"   # apostrofo sicuro per JS dentro JSON raw

def _js(s: str) -> str:
    """Sostituisce ' con \\u0027 nel codice JS prima dell'iniezione nel raw."""
    return s.replace("'", _Q4)

_RIAS_SCRIPT = (
    "<style>"
    "#rias-ov{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;"
    "align-items:center;justify-content:center;padding:20px;box-sizing:border-box;"
    "backdrop-filter:blur(3px)}"
    "#rias-ov.open{display:flex}"
    "#rias-box{background:#fff;border-radius:16px;max-width:620px;width:100%;"
    "max-height:88vh;overflow-y:auto;padding:28px 28px 24px;"
    "box-shadow:0 24px 60px rgba(0,0,0,.3);position:relative;font-family:inherit}"
    "#rias-chip{display:inline-flex;align-items:center;gap:6px;font-size:11px;"
    "font-weight:600;padding:4px 12px;border-radius:100px;background:#f0f4ff;"
    "color:#1B4FCA;margin-bottom:12px}"
    "#rias-ogg{font-size:15px;font-weight:700;line-height:1.4;color:#1a1a1a;margin:0 0 14px}"
    "#rias-txt{font-size:13.5px;line-height:1.85;color:#444;margin:0 0 20px}"
    "#rias-ft{font-size:11px;color:#aaa;border-top:1px solid #f0f0f0;"
    "padding-top:14px;display:flex;justify-content:space-between;align-items:center}"
    "#rias-lnk{font-size:12px;font-weight:600;color:#1B4FCA;text-decoration:none}"
    "#rias-x{position:absolute;top:14px;right:14px;width:28px;height:28px;"
    "border:none;background:#f0f0f0;border-radius:50%;cursor:pointer;font-size:15px;"
    "display:flex;align-items:center;justify-content:center;color:#555;line-height:1}"
    "<\\/style>"
    "<div id=rias-ov>"
    "<div id=rias-box>"
    "<button id=rias-x>&#x2715;<\\/button>"
    "<div id=rias-chip><\\/div>"
    "<p id=rias-ogg><\\/p>"
    "<p id=rias-txt><\\/p>"
    "<div id=rias-ft>"
    "<span id=rias-dt><\\/span>"
    "<a id=rias-lnk href=# target=_blank>Leggi atto completo &#x2192;<\\/a>"
    "<\\/div>"
    "<\\/div>"
    "<\\/div>"
) + _js(
    "<script>"
    "(function(){"
    "var ov=document.getElementById('rias-ov');"
    "function openModal(){ov.classList.add('open');document.body.style.overflow='hidden';}"
    "function closeModal(){ov.classList.remove('open');document.body.style.overflow='';}"
    "document.getElementById('rias-x').onclick=closeModal;"
    "ov.onclick=function(e){if(e.target===ov)closeModal();};"
    "document.onkeydown=function(e){if(e.key==='Escape')closeModal();};"
    "document.addEventListener('click',function(e){"
    "var b=e.target.closest('.rias-toggle');"
    "if(!b)return;"
    "var w=b.closest('[data-rias]');"
    "if(!w)return;"
    "document.getElementById('rias-chip').textContent=w.dataset.tipo||'';"
    "document.getElementById('rias-ogg').textContent=w.dataset.oggetto||'';"
    "document.getElementById('rias-txt').textContent=w.dataset.testo||'';"
    "document.getElementById('rias-dt').textContent=(w.dataset.data||'')+' · '+(w.dataset.numero||'');"
    "document.getElementById('rias-lnk').href=w.dataset.url||'#';"
    "openModal();"
    "});"
    "})();"
    "<\\/script>"
)

PATCH4_MARKER = "<\\u002Fhead>"   # nel raw JSON: </head>
PATCH4_NEW = _RIAS_SCRIPT + PATCH4_MARKER

# PATCH 3 — JS: aggiunge oggettoFull, riassuntoFull, hasTruncation, url_archivio
# PATCH3_OLD_A = stato originale (nessuna patch applicata)
# PATCH3_OLD_B = stato intermedio con riassunto: (invece di riassuntoFull:)
PATCH3_OLD_A = (
    "riassuntoShort: a.riassunto.length > 165 ? a.riassunto.slice(0, 165) + '…' : a.riassunto,\\n"
    "        hasRiassunto: showRias,"
)
PATCH3_OLD_B = (
    "riassuntoShort: a.riassunto.length > 165 ? a.riassunto.slice(0, 165) + '…' : a.riassunto,\\n"
    "        riassunto: a.riassunto,\\n"
    "        hasTruncation: a.riassunto.length > 165 ? 'inline' : 'none',\\n"
    "        hasRiassunto: showRias,\\n"
    "        url_archivio: a.url_archivio || '',\\n"
    "        hasArchivio: !!(a.url_archivio),"
)
# Stato target corretto: oggettoFull + riassuntoFull
PATCH3_NEW = (
    "riassuntoShort: a.riassunto.length > 165 ? a.riassunto.slice(0, 165) + '…' : a.riassunto,\\n"
    "        riassuntoFull: a.riassunto,\\n"
    "        oggettoFull: a.oggetto,\\n"
    "        hasTruncation: a.riassunto.length > 165 ? 'inline' : 'none',\\n"
    "        hasRiassunto: showRias,\\n"
    "        url_archivio: a.url_archivio || '',\\n"
    "        hasArchivio: !!(a.url_archivio),"
)
# Alias per compatibilità
PATCH3_OLD = PATCH3_OLD_A


def applica_patch_raw(raw: str) -> str:
    """Applica le patch direttamente sul raw del file (idempotente)."""
    if "rias-wrap" in raw:
        print("  · Patch 1 già corretta (modale data-*)")
    elif PATCH1_OLD_B in raw:
        raw = raw.replace(PATCH1_OLD_B, PATCH1_NEW)
        print("  ✓ Patch 1 aggiornata (span v2→modale data-*)")
    elif PATCH1_OLD_A in raw:
        raw = raw.replace(PATCH1_OLD_A, PATCH1_NEW)
        print("  ✓ Patch 1 (modale data-*) applicata")
    elif "rias-short" in raw and "rias-toggle" in raw:
        # Variante con onclick inline o struttura diversa — usa regex
        import re as _re
        m_p1 = _re.search(
            r'<p style=\\"font-size:12px[^>]+>.*?<\\u002Fbutton>',
            raw, _re.DOTALL
        )
        if m_p1:
            raw = raw[:m_p1.start()] + PATCH1_NEW + raw[m_p1.end():]
            print("  ✓ Patch 1 aggiornata (regex→modale data-*)")
        else:
            print("  ⚠ Patch 1: regex non trova il blocco")
    else:
        print("  ⚠ Patch 1: target non trovato")

    if PATCH2_OLD in raw:
        raw = raw.replace(PATCH2_OLD, PATCH2_NEW)
        print("  ✓ Patch 2 (link archivio) applicata")
    elif "hasArchivio" in raw:
        print("  · Patch 2 già presente")
    else:
        print("  ⚠ Patch 2: target non trovato")

    # Check su stringa specifica nel mapping JS (non nel template HTML)
    if "oggettoFull: a.oggetto" in raw and "riassuntoFull: a.riassunto" in raw:
        print("  · Patch 3 già presente")
    elif PATCH3_OLD_B in raw:
        raw = raw.replace(PATCH3_OLD_B, PATCH3_NEW)
        print("  ✓ Patch 3 aggiornata (riassunto→riassuntoFull + oggettoFull)")
    elif PATCH3_OLD_A in raw:
        raw = raw.replace(PATCH3_OLD_A, PATCH3_NEW)
        print("  ✓ Patch 3 (JS dati) applicata")
    else:
        print("  ⚠ Patch 3: target non trovato")

    # PATCH 4 — inietta modale overlay + event listener nel <head>
    # Check su 'rias-ov' (id univoco del nuovo modale)
    if 'id=rias-ov' not in raw and 'id=\\"rias-ov\\"' not in raw:
        if PATCH4_MARKER in raw:
            # Rimuovi eventuale vecchio script (varianti precedenti)
            import re as _re
            raw = _re.sub(r'<script>function riasToggle[^<]*<\\/script>', '', raw)
            # Rimuovi vecchio modale rias-overlay se presente
            raw = _re.sub(
                r'<style>#rias-overlay\{.*?<\\/script>',
                '',
                raw,
                flags=_re.DOTALL
            )
            raw = raw.replace(PATCH4_MARKER, PATCH4_NEW, 1)
            print("  ✓ Patch 4 (modale overlay) iniettata nel <head>")
        else:
            print("  ⚠ Patch 4: </head> non trovato nel template")
    else:
        print("  · Patch 4 già presente")

    return raw


# ── Helpers escape per ALL_ATTI ──────────────────────────────────────────────

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


def sv(s: str) -> str:
    """
    Produce un letterale JS con virgolette singole, safe per stare
    dentro una stringa JSON con delimitatori doppi.

    Catena di escape per apostrofi:
      Nel file raw:      \\u0027
      Dopo JSON decode:  \\u0027  (\\ → \, poi u0027 letterale)
      Nel JS:            '   (unicode escape → apostrofo ')
    Stessa logica per " → \\u0022.
    """
    s = str(s) if s else ""
    s = s.replace("\\", "\\\\")
    s = s.replace("\n", " ").replace("\r", "")
    # Apostrofi (ASCII e tipografici)
    s = s.replace("'",  "\\\\u0027")   # ASCII '
    s = s.replace("’", "\\\\u0027")   # RIGHT SINGLE QUOTATION MARK
    s = s.replace("‘", "\\\\u0027")   # LEFT SINGLE QUOTATION MARK
    # Virgolette doppie (ASCII e tipografiche)
    s = s.replace('"',  "\\\\u0022")   # ASCII "
    s = s.replace("“", "\\\\u0022")   # LEFT DOUBLE QUOTATION MARK
    s = s.replace("”", "\\\\u0022")   # RIGHT DOUBLE QUOTATION MARK
    return f"'{s}'"


def atti_to_js_block(atti: list[dict]) -> str:
    """Converte la lista degli atti nel blocco JS ALL_ATTI."""
    righe = []
    for a in atti:
        tipo         = sv(tipo_breve(a.get("tipo", "Atto")))
        tipo_norm    = sv(a.get("tipo_norm", ""))
        numero       = sv(a.get("numero_raw", ""))
        data         = sv(fmt_data(a.get("data_inizio", "")))
        dk           = sv((a.get("data_inizio", "") or "")[:10])
        oggetto      = sv((a.get("oggetto", "") or "")[:200])
        riassunto    = sv(a.get("riassunto", "") or "")
        url          = sv(a.get("url_dettaglio", "") or "")
        url_archivio = sv(a.get("url_archivio", "") or "")

        riga = (
            f"    {{ tipo: {tipo}, tipoNorm: {tipo_norm}, "
            f"numero: {numero}, data: {data}, dk: {dk},\\n"
            f"      oggetto: {oggetto},\\n"
            f"      riassunto: {riassunto},\\n"
            f"      url: {url},\\n"
            f"      url_archivio: {url_archivio} }}"
        )
        righe.append(riga)

    corpo = ",\\n".join(righe)
    return f"ALL_ATTI = [\\n{corpo},\\n  ]"


# ── Core ──────────────────────────────────────────────────────────────────────

def aggiorna_standalone(atti: list[dict]) -> bool:
    if not STANDALONE.exists():
        print(f"File non trovato: {STANDALONE}")
        return False

    with open(STANDALONE, "r", encoding="utf-8") as f:
        content = f.read()

    # Trova i limiti del tag template
    TAG = '<script type="__bundler/template">'
    tag_start = content.find(TAG)
    if tag_start == -1:
        print("ERRORE: tag __bundler/template non trovato")
        return False
    tag_content_start = tag_start + len(TAG)
    tag_end = content.find("</script>", tag_content_start)

    raw = content[tag_content_start:tag_end]

    # Verifica che il JSON di partenza sia valido
    try:
        json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"ERRORE: template JSON non valido prima delle patch: {e}")
        return False

    # Applica patch UI sul raw
    print("Applicazione patch al template:")
    raw = applica_patch_raw(raw)

    # Sostituisce il blocco ALL_ATTI nel raw
    pattern = re.compile(r"ALL_ATTI = \[.*?\]", re.DOTALL)
    match = pattern.search(raw)
    if not match:
        print("ERRORE: blocco ALL_ATTI non trovato nel raw")
        return False

    print(f"  Blocco ALL_ATTI: posizione {match.start()}–{match.end()}")
    print(f"  Atti da iniettare: {len(atti)}")

    nuovo_blocco = atti_to_js_block(atti)
    raw = raw[:match.start()] + nuovo_blocco + raw[match.end():]

    # Sostituisce ATTI_COUNTS con i conteggi reali per data (campo dk)
    # Necessario per evidenziare i giorni con atti nel calendario
    counts: dict[str, int] = {}
    for a in atti:
        dk = (a.get("data_inizio") or "")[:10]
        if dk:
            counts[dk] = counts.get(dk, 0) + 1
    counts_js = json.dumps(counts, ensure_ascii=False)
    pattern_counts = re.compile(r"ATTI_COUNTS = \{[^}]*\}")
    match_counts = pattern_counts.search(raw)
    if match_counts:
        raw = raw[:match_counts.start()] + f"ATTI_COUNTS = {counts_js}" + raw[match_counts.end():]
        print(f"  ATTI_COUNTS aggiornato: {len(counts)} date con atti")
    else:
        print("  ATTENZIONE: ATTI_COUNTS non trovato nel template, calendario non aggiornato")

    # Riassembla il file
    new_content = content[:tag_content_start] + raw + content[tag_end:]

    with open(STANDALONE, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"File aggiornato: {STANDALONE} ({len(new_content)//1024} KB)")
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
        # Verifica finale
        with open(STANDALONE, "r", encoding="utf-8") as f:
            content = f.read()
        TAG = '<script type="__bundler/template">'
        ts = content.find(TAG) + len(TAG)
        te = content.find("</script>", ts)
        try:
            json.loads(content[ts:te])
            print("✅ JSON template VALIDO")
        except json.JSONDecodeError as e:
            print(f"❌ JSON template INVALIDO: {e}")
            pos = e.pos
            raw = content[ts:te]
            print(f"Contesto: {repr(raw[max(0,pos-100):pos+100])}")
        print(f"Aggiornamento completato: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    else:
        print("Aggiornamento fallito.")


if __name__ == "__main__":
    main()
