"""
genera_sito.py — Genera il sito statico GitHub Pages dall'archivio degli atti.
Output: docs/index.html

Layout: header card istituzionale (proposta C) + griglia card compatte (proposta B)
Stile: claude.ai — superfici bianche, bordi 0.5px, font di sistema, zero ombre decorative.
"""

import json
import html
import calendar
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict

ATTI_JSON = Path("data/atti.json")
DOCS_DIR  = Path("docs")
OUTPUT    = DOCS_DIR / "index.html"

# Classi CSS per tipo atto → (pill-class, icona-tabler)
TIPO_CONFIG = {
    "delibera":            ("pill-acc", "ti-building-bank",    "Delibera"),
    "determinazione":      ("pill-suc", "ti-clipboard-list",   "Determinazione"),
    "ordinanza":           ("pill-dan", "ti-alert-triangle",   "Ordinanza"),
    "avviso":              ("pill-war", "ti-speakerphone",     "Avviso"),
    "bando":               ("pill-pro", "ti-file-text",        "Bando"),
    "appalto":             ("pill-pro", "ti-hammer",           "Appalto"),
    "variazione-bilancio": ("pill-acc", "ti-chart-bar",        "Var. bilancio"),
}

def tipo_config(tipo_norm: str):
    for k, v in TIPO_CONFIG.items():
        if k in (tipo_norm or "").lower():
            return v
    return ("pill-neu", "ti-file", "Atto")

def tipo_breve(tipo_raw: str) -> str:
    if "/" in tipo_raw:
        return tipo_raw.split("/")[-1].strip().title()
    return tipo_raw.strip().title()

def fmt_data(iso: str) -> str:
    if not iso or len(iso) < 10:
        return iso or ""
    y, m, d = iso[:10].split("-")
    return f"{d}/{m}/{y}"


def card_html(atto: dict) -> str:
    tipo_norm = atto.get("tipo_norm", "")
    pill_cls, icon, label = tipo_config(tipo_norm)
    tipo_display = tipo_breve(atto.get("tipo", "Atto"))
    num   = html.escape(atto.get("numero_raw", ""))
    ogg   = html.escape(atto.get("oggetto", "")[:160])
    rias  = html.escape(atto.get("riassunto", "")[:300])
    url   = atto.get("url_dettaglio", "") or ""
    data  = fmt_data(atto.get("data_inizio", ""))

    link = (f'<a href="{html.escape(url)}" target="_blank" rel="noopener" class="card-link">'
            f'Leggi <i class="ti ti-arrow-right" aria-hidden="true"></i></a>') if url else ""

    riassunto_html = f'<p class="card-rias">{rias}</p>' if rias else ""

    return f"""<div class="card">
  <div class="pill {pill_cls}"><i class="ti {icon}" aria-hidden="true"></i>{html.escape(tipo_display)}</div>
  <h4 class="card-title">{ogg}</h4>
  {riassunto_html}
  <div class="card-foot">
    <span class="card-data">{data} · {num}</span>
    {link}
  </div>
</div>"""


def cal_cell_js_data(per_data: dict, anno: int, mese: int) -> str:
    subset = {}
    for dk, lista in per_data.items():
        if dk.startswith(f"{anno}-{mese:02d}"):
            subset[dk] = [
                {
                    "tipo":      tipo_breve(a.get("tipo", "Atto")),
                    "tipo_norm": a.get("tipo_norm", ""),
                    "numero":    a.get("numero_raw", ""),
                    "oggetto":   a.get("oggetto", "")[:200],
                    "riassunto": a.get("riassunto", ""),
                    "url":       a.get("url_dettaglio", "") or "",
                    "data":      fmt_data(a.get("data_inizio", "")),
                }
                for a in lista
            ]
    return json.dumps(subset, ensure_ascii=False)


def genera_html(atti: list[dict]) -> str:
    oggi        = date.today()
    anno        = oggi.year
    mese        = oggi.month
    data_agg    = datetime.now().strftime("%d/%m/%Y %H:%M")
    nome_mese   = datetime(anno, mese, 1).strftime("%B %Y").capitalize()

    # Raggruppa per data
    per_data: dict[str, list] = defaultdict(list)
    for a in atti:
        d = a.get("data_inizio", "")
        if d:
            per_data[d].append(a)

    # Sezione recenti
    atti_oggi = per_data.get(oggi.isoformat(), [])
    atti_settimana = []
    for i in range(7):
        atti_settimana.extend(per_data.get((oggi - timedelta(days=i)).isoformat(), []))

    if atti_oggi:
        titolo_sec = f"Oggi — {fmt_data(oggi.isoformat())}"
        atti_recenti = atti_oggi
    elif atti_settimana:
        titolo_sec = "Ultimi 7 giorni"
        atti_recenti = atti_settimana[:12]
    else:
        titolo_sec = "Atti più recenti"
        atti_recenti = atti[:12]

    cards_html = "\n".join(card_html(a) for a in atti_recenti)
    if not cards_html:
        cards_html = '<p class="empty">Nessun atto pubblicato di recente.</p>'

    # Statistiche
    n_tot   = len(atti)
    n_mese  = sum(1 for a in atti if (a.get("data_inizio") or "").startswith(f"{anno}-{mese:02d}"))
    n_oggi  = len(atti_oggi)

    # Calendario
    cal_matrix = calendar.monthcalendar(anno, mese)
    giorni_hdr = "".join(f'<div class="ch">{g}</div>' for g in ["Lun","Mar","Mer","Gio","Ven","Sab","Dom"])

    celle = ""
    for settimana in cal_matrix:
        for g in settimana:
            if g == 0:
                celle += '<div class="cd vuoto"></div>'
                continue
            dk = f"{anno}-{mese:02d}-{g:02d}"
            n  = len(per_data.get(dk, []))
            cls = "cd"
            if g == oggi.day:
                cls += " oggi"
            if n > 0:
                cls += " ha-atti"
                dot = f'<span class="dot">{n}</span>'
                celle += f'<div class="{cls}" onclick="apriGiorno(\'{dk}\')">{g}{dot}</div>'
            else:
                celle += f'<div class="{cls}">{g}</div>'

    atti_js = cal_cell_js_data(per_data, anno, mese)

    return f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Albo Pretorio — Comune di Pieve Emanuele</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">
<style>
/* ── reset ── */
*{{box-sizing:border-box;margin:0;padding:0}}

/* ── tokens ── */
:root{{
  --bg:       #f5f4f0;
  --surface:  #ffffff;
  --border:   rgba(0,0,0,.1);
  --border-s: rgba(0,0,0,.18);
  --text:     #1a1a1a;
  --muted:    #6b6b6b;
  --hint:     #9b9b9b;
  --acc-bg:   #e8f0fe;
  --acc-fg:   #1a56c4;
  --dan-bg:   #fde8e8;
  --dan-fg:   #b91c1c;
  --suc-bg:   #e6f4ea;
  --suc-fg:   #166534;
  --war-bg:   #fef3cd;
  --war-fg:   #92400e;
  --pro-bg:   #f0ebfe;
  --pro-fg:   #5b21b6;
  --radius:   10px;
}}

body{{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
  background:var(--bg);
  color:var(--text);
  font-size:14px;
  line-height:1.5;
  -webkit-font-smoothing:antialiased;
}}

/* ── layout ── */
.wrap{{max-width:860px;margin:0 auto;padding:24px 16px 64px}}

/* ── header card ── */
.header-card{{
  background:var(--surface);
  border:0.5px solid var(--border);
  border-radius:var(--radius);
  padding:20px 24px;
  display:flex;
  gap:16px;
  align-items:flex-start;
  margin-bottom:28px;
}}
.header-icon{{
  width:44px;height:44px;border-radius:10px;
  background:var(--acc-bg);
  display:flex;align-items:center;justify-content:center;
  flex-shrink:0;
  color:var(--acc-fg);font-size:22px;
}}
.header-title{{font-size:18px;font-weight:500;color:var(--text);margin-bottom:2px}}
.header-sub{{font-size:13px;color:var(--muted)}}
.header-pills{{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}}
.hp{{
  font-size:12px;padding:3px 10px;border-radius:20px;
  background:var(--bg);border:0.5px solid var(--border);
  color:var(--muted);white-space:nowrap;
}}

/* ── section heading ── */
.sec{{
  font-size:11px;font-weight:500;letter-spacing:.07em;
  text-transform:uppercase;color:var(--hint);
  margin-bottom:12px;padding-bottom:8px;
  border-bottom:0.5px solid var(--border);
}}

/* ── cards grid ── */
.grid{{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(240px,1fr));
  gap:8px;
  margin-bottom:28px;
}}
.card{{
  background:var(--surface);
  border:0.5px solid var(--border);
  border-radius:var(--radius);
  padding:14px 16px;
  display:flex;flex-direction:column;
  transition:border-color .12s;
}}
.card:hover{{border-color:var(--border-s)}}

/* pill tipo */
.pill{{
  display:inline-flex;align-items:center;gap:4px;
  font-size:11px;font-weight:500;
  padding:3px 9px;border-radius:20px;
  margin-bottom:8px;width:fit-content;
}}
.pill i{{font-size:11px}}
.pill-acc{{background:var(--acc-bg);color:var(--acc-fg)}}
.pill-dan{{background:var(--dan-bg);color:var(--dan-fg)}}
.pill-suc{{background:var(--suc-bg);color:var(--suc-fg)}}
.pill-war{{background:var(--war-bg);color:var(--war-fg)}}
.pill-pro{{background:var(--pro-bg);color:var(--pro-fg)}}
.pill-neu{{background:var(--bg);color:var(--muted);border:0.5px solid var(--border)}}

.card-title{{
  font-size:13px;font-weight:500;color:var(--text);
  line-height:1.45;margin-bottom:6px;flex:1;
}}
.card-rias{{
  font-size:12px;color:var(--muted);line-height:1.55;margin-bottom:8px;
}}
.card-foot{{
  display:flex;justify-content:space-between;align-items:center;
  margin-top:auto;padding-top:10px;
  border-top:0.5px solid var(--border);
}}
.card-data{{font-size:11px;color:var(--hint)}}
.card-link{{
  font-size:11px;color:var(--acc-fg);
  text-decoration:none;display:inline-flex;align-items:center;gap:2px;
}}
.card-link:hover{{text-decoration:underline}}
.empty{{font-size:13px;color:var(--hint);padding:12px 0;font-style:italic}}

/* ── calendario ── */
.cal-grid{{display:grid;grid-template-columns:repeat(7,1fr);gap:3px}}
.ch{{font-size:11px;color:var(--hint);text-align:center;padding:5px 0;font-weight:500}}
.cd{{
  font-size:13px;text-align:center;padding:9px 2px;
  border-radius:6px;color:var(--muted);
  position:relative;cursor:default;user-select:none;
}}
.cd.ha-atti{{
  color:var(--acc-fg);font-weight:500;cursor:pointer;
  background:var(--acc-bg);
}}
.cd.ha-atti:hover{{opacity:.85}}
.cd.oggi{{
  outline:1.5px solid var(--border-s);
  color:var(--text);font-weight:500;
}}
.cd.vuoto{{pointer-events:none}}
.dot{{
  position:absolute;bottom:3px;left:50%;transform:translateX(-50%);
  font-size:9px;color:var(--acc-fg);font-weight:700;line-height:1;
}}

/* ── pannello giorno ── */
#pannello{{
  display:none;margin-top:14px;
  background:var(--surface);
  border:0.5px solid var(--border);
  border-radius:var(--radius);padding:16px;
}}
#pannello-hdr{{
  display:flex;justify-content:space-between;align-items:center;
  margin-bottom:14px;
}}
#pannello-hdr h4{{font-size:15px;font-weight:500;color:var(--text)}}
#pannello-close{{
  background:none;border:none;cursor:pointer;
  font-size:18px;color:var(--hint);line-height:1;padding:0 2px;
}}
#pannello-body .card{{margin-bottom:8px}}
#pannello-body .card:last-child{{margin-bottom:0}}

/* ── footer ── */
footer{{
  text-align:center;padding:32px 16px;
  font-size:12px;color:var(--hint);
  border-top:0.5px solid var(--border);margin-top:40px;
}}
footer a{{color:var(--muted);text-decoration:none}}
footer a:hover{{text-decoration:underline}}

/* ── mobile ── */
@media(max-width:520px){{
  .grid{{grid-template-columns:1fr}}
  .header-card{{flex-direction:column;gap:12px}}
  .cd{{padding:7px 1px;font-size:12px}}
}}
</style>
</head>
<body>
<div class="wrap">

  <!-- HEADER -->
  <div class="header-card">
    <div class="header-icon"><i class="ti ti-building-community" aria-hidden="true"></i></div>
    <div style="flex:1">
      <div class="header-title">Albo Pretorio</div>
      <div class="header-sub">Comune di Pieve Emanuele (MI) · aggiornato {data_agg}</div>
      <div class="header-pills">
        <span class="hp">{n_tot} atti in archivio</span>
        <span class="hp">{n_mese} questo mese</span>
        <span class="hp">{n_oggi} oggi</span>
      </div>
    </div>
  </div>

  <!-- ATTI RECENTI -->
  <div class="sec">{titolo_sec}</div>
  <div class="grid">
    {cards_html}
  </div>

  <!-- CALENDARIO -->
  <div class="sec">Calendario — {nome_mese}</div>
  <div class="cal-grid">
    {giorni_hdr}
    {celle}
  </div>

  <!-- PANNELLO GIORNO -->
  <div id="pannello">
    <div id="pannello-hdr">
      <h4 id="pannello-titolo"></h4>
      <button id="pannello-close" onclick="chiudiPannello()" aria-label="Chiudi">&#x2715;</button>
    </div>
    <div id="pannello-body"></div>
  </div>

</div>

<footer>
  Dati ufficiali dall'<a href="https://pieveemanuele.trasparenza-valutazione-merito.it/web/trasparenza/papca-ap/-/papca/igrid/0/Albo_pretorio/" target="_blank" rel="noopener">Albo Pretorio del Comune di Pieve Emanuele</a>.
  Progetto open source su <a href="https://github.com/NT0wers84/albo-pretorio" target="_blank" rel="noopener">GitHub</a>.
</footer>

<script>
const ATTI = {atti_js};

const PILL = {{
  delibera:   ['pill-acc','ti-building-bank'],
  determinazione:['pill-suc','ti-clipboard-list'],
  ordinanza:  ['pill-dan','ti-alert-triangle'],
  avviso:     ['pill-war','ti-speakerphone'],
  bando:      ['pill-pro','ti-file-text'],
  appalto:    ['pill-pro','ti-hammer'],
  'variazione-bilancio':['pill-acc','ti-chart-bar'],
}};

function pillFor(tipo_norm){{
  for(const [k,v] of Object.entries(PILL)){{
    if((tipo_norm||'').toLowerCase().includes(k)) return v;
  }}
  return ['pill-neu','ti-file'];
}}

function cardHTML(a){{
  const [cls,icon] = pillFor(a.tipo_norm);
  const link = a.url
    ? `<a href="${{a.url}}" target="_blank" rel="noopener" class="card-link">Leggi <i class="ti ti-arrow-right" aria-hidden="true"></i></a>`
    : '';
  const rias = a.riassunto
    ? `<p class="card-rias">${{a.riassunto}}</p>`
    : '';
  return `<div class="card">
    <div class="pill ${{cls}}"><i class="ti ${{icon}}" aria-hidden="true"></i>${{a.tipo}}</div>
    <h4 class="card-title">${{a.oggetto}}</h4>
    ${{rias}}
    <div class="card-foot">
      <span class="card-data">${{a.data}} · ${{a.numero}}</span>
      ${{link}}
    </div>
  </div>`;
}}

function apriGiorno(dk){{
  const lista = ATTI[dk];
  if(!lista||!lista.length) return;
  const [,, d] = dk.split('-');
  document.getElementById('pannello-titolo').textContent =
    'Atti del '+d+'/'+dk.slice(5,7)+'/'+dk.slice(0,4)+' ('+lista.length+')';
  document.getElementById('pannello-body').innerHTML = lista.map(cardHTML).join('');
  const p = document.getElementById('pannello');
  p.style.display = 'block';
  p.scrollIntoView({{behavior:'smooth',block:'nearest'}});
}}

function chiudiPannello(){{
  document.getElementById('pannello').style.display='none';
}}
</script>
</body>
</html>"""


def main():
    DOCS_DIR.mkdir(exist_ok=True)
    atti = []
    if ATTI_JSON.exists():
        with open(ATTI_JSON, "r", encoding="utf-8") as f:
            atti = json.load(f)
    print(f"Genero sito con {len(atti)} atti...")
    content = genera_html(atti)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Sito generato: {OUTPUT} ({len(content)//1024} KB)")


if __name__ == "__main__":
    main()
