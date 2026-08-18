"""
genera_sito.py — Genera docs/index.html dall'archivio atti.
Sostituisce aggiorna_standalone.py: niente patch su bundle, HTML rigenerato ogni run.

Palette e spaziature allineate al design originale (Claude Design bundle,
vedi backup pre-refactor) per mantenere continuità estetica pur generando
l'HTML da zero invece che tramite patch.
"""

import json
import html
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict

ATTI_JSON = Path("data/atti.json")
DOCS_DIR  = Path("docs")
OUTPUT    = DOCS_DIR / "index.html"
SITO_URL  = "https://nt0wers84.github.io/albo-pretorio/"
FEED_URL  = SITO_URL + "feed.xml"
NEWSLETTER_EMBED = "https://buttondown.com/api/emails/embed-subscribe/albo-pretorio-pe"

TIPO_CONFIG = {
    "delibera":            ("pill-acc", "ti-building-bank"),
    "determinazione":      ("pill-suc", "ti-clipboard-list"),
    "ordinanza":           ("pill-dan", "ti-alert-triangle"),
    "avviso":              ("pill-war", "ti-speakerphone"),
    "bando":               ("pill-pro", "ti-file-text"),
    "appalto":             ("pill-pro", "ti-hammer"),
    "variazione-bilancio": ("pill-acc", "ti-chart-bar"),
}

FILTRI = [
    ("", "Tutti"),
    ("delibera", "Delibere"),
    ("determinazione", "Determinazioni"),
    ("ordinanza", "Ordinanze"),
    ("avviso", "Avvisi"),
    ("bando", "Bandi"),
    ("appalto", "Appalti"),
    ("variazione-bilancio", "Var. bilancio"),
]


def tipo_breve(tipo_raw: str) -> str:
    if "/" in tipo_raw:
        return tipo_raw.split("/")[-1].strip().title()
    return (tipo_raw or "Atto").strip().title()


def fmt_data(iso: str) -> str:
    if not iso or len(iso) < 10:
        return iso or ""
    y, m, d = iso[:10].split("-")
    return f"{d}/{m}/{y}"


def atto_to_js(a: dict, idx: int) -> dict:
    return {
        "idx": idx,
        "tipo": tipo_breve(a.get("tipo", "Atto")),
        "tipoNorm": a.get("tipo_norm", ""),
        "numero": a.get("numero_raw", ""),
        "data": fmt_data(a.get("data_inizio", "")),
        "dk": (a.get("data_inizio") or "")[:10],
        "oggetto": (a.get("oggetto") or "")[:200],
        "riassunto": a.get("riassunto") or "",
        "url": a.get("url_dettaglio") or "",
    }


def genera_html(atti: list[dict]) -> str:
    oggi = date.today()
    anno = oggi.year
    mese = oggi.month
    data_agg = datetime.now().strftime("%d/%m/%Y %H:%M")

    n_tot = len(atti)
    n_mese = sum(1 for a in atti if (a.get("data_inizio") or "").startswith(f"{anno}-{mese:02d}"))
    n_oggi = sum(1 for a in atti if (a.get("data_inizio") or "")[:10] == oggi.isoformat())

    per_data: dict[str, list] = defaultdict(list)
    for a in atti:
        d = (a.get("data_inizio") or "")[:10]
        if d:
            per_data[d].append(a)

    atti_counts = {k: len(v) for k, v in per_data.items()}
    all_atti_js = json.dumps([atto_to_js(a, i) for i, a in enumerate(atti)], ensure_ascii=False)
    atti_counts_js = json.dumps(atti_counts, ensure_ascii=False)

    # Il calendario è renderizzato interamente lato client (JS) per permettere
    # la navigazione avanti/indietro tra i mesi senza ricaricare la pagina.

    filtri_html = "".join(
        f'<button class="chip{" active" if v == "" else ""}" data-tipo="{html.escape(v)}">{html.escape(label)}</button>'
        for v, label in FILTRI
    )

    return f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Albo in chiaro — Pieve Emanuele</title>
<meta name="description" content="Atti dell'Albo Pretorio del Comune di Pieve Emanuele spiegati in parole semplici. Progetto civico indipendente.">
<link rel="alternate" type="application/rss+xml" title="Albo Pretorio — Pieve Emanuele" href="{FEED_URL}">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#F2F1ED;--surface:#fff;--panel-bg:#F9F8F5;
  --border:rgba(0,0,0,.08);--border-soft:rgba(0,0,0,.07);
  --text:#141412;--muted:#636158;--hint:#B0AEA8;
  --acc:#1B4FCA;--acc-bg:#EEF3FD;--acc-border:#C7D9FA;
  --active:#17261E;--header:#123785;
  --dan-bg:#fde8e8;--dan-fg:#b91c1c;
  --suc-bg:#e6f4ea;--suc-fg:#166534;--war-bg:#fef3cd;--war-fg:#92400e;
  --pro-bg:#f0ebfe;--pro-fg:#5b21b6;--radius:14px;
}}
body{{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
  background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;
  -webkit-font-smoothing:antialiased;
}}
header{{
  background:var(--header);color:#fff;text-align:center;
  padding:56px 24px 48px;
}}
header .cap{{
  font-size:11px;letter-spacing:.14em;text-transform:uppercase;
  color:rgba(255,255,255,.9);font-weight:600;margin-bottom:18px;
}}
header h1{{
  font-family:'DM Serif Display',Georgia,serif;font-size:48px;font-weight:400;
  letter-spacing:-.01em;line-height:1;margin-bottom:14px;
}}
header .sub{{font-size:15px;color:#fff;opacity:.95;margin-bottom:7px}}
header .desc{{
  font-size:12px;color:rgba(255,255,255,.85);max-width:560px;
  margin:0 auto 40px;line-height:1.7;letter-spacing:.01em;
}}
.stats{{
  display:inline-flex;border-radius:16px;overflow:hidden;
  border:1px solid rgba(255,255,255,.35);background:rgba(255,255,255,.15);
}}
.stat{{padding:20px 40px;min-width:90px}}
.stat+.stat{{border-left:1px solid rgba(255,255,255,.28)}}
.stat-n{{font-size:34px;font-weight:400;line-height:1;margin-bottom:7px;letter-spacing:-.02em}}
.stat-l{{
  font-size:10px;text-transform:uppercase;letter-spacing:.09em;
  color:rgba(255,255,255,.9);font-weight:600;
}}
main{{max-width:920px;margin:0 auto;padding:38px 20px 80px}}
.sec-label{{
  font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  color:var(--hint);margin-bottom:16px;padding-bottom:10px;
  border-bottom:0.5px solid rgba(0,0,0,.09);
}}
.chips{{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:32px;align-items:center}}
.chip{{
  font-size:13px;padding:8px 18px;border-radius:100px;cursor:pointer;
  border:1.5px solid rgba(0,0,0,.1);background:var(--surface);color:var(--muted);
  font-family:inherit;font-weight:500;line-height:1.2;outline:none;transition:all .15s;
}}
.chip.active{{background:var(--active);color:#fff;border-color:var(--active)}}
.toolbar{{margin-bottom:24px;position:relative}}
.toolbar i.ti-search{{
  position:absolute;left:14px;top:50%;transform:translateY(-50%);
  font-size:15px;color:var(--hint);pointer-events:none;
}}
.toolbar input{{
  width:100%;padding:12px 14px 12px 42px;font-size:14px;border:0.5px solid rgba(0,0,0,.1);
  border-radius:12px;background:var(--surface);font-family:inherit;
  outline:none;color:var(--text);transition:border-color .15s;
}}
.toolbar input:focus{{border-color:rgba(23,38,30,.4);box-shadow:0 0 0 3px rgba(23,38,30,.07)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;margin-bottom:44px}}
.card{{
  background:var(--surface);border:0.5px solid var(--border);border-radius:var(--radius);
  padding:18px 20px;display:flex;flex-direction:column;
  transition:border-color .12s,box-shadow .12s;
}}
.card:hover{{border-color:rgba(0,0,0,.18);box-shadow:0 2px 10px rgba(0,0,0,.06)}}
.card.panel-card{{background:var(--panel-bg);border-color:var(--border-soft);padding:16px 18px}}
.pill{{
  display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:700;
  padding:4px 10px;border-radius:100px;margin-bottom:10px;width:fit-content;
  letter-spacing:.06em;text-transform:uppercase;
}}
.pill i{{font-size:11px}}
.pill-acc{{background:var(--acc-bg);color:var(--acc)}}
.pill-dan{{background:var(--dan-bg);color:var(--dan-fg)}}
.pill-suc{{background:var(--suc-bg);color:var(--suc-fg)}}
.pill-war{{background:var(--war-bg);color:var(--war-fg)}}
.pill-pro{{background:var(--pro-bg);color:var(--pro-fg)}}
.pill-neu{{background:var(--bg);color:var(--muted)}}
.card-title{{font-size:13px;font-weight:500;line-height:1.55;color:var(--text);margin-bottom:8px;flex:1}}
.card-rias{{font-size:12px;color:var(--muted);line-height:1.7;margin-bottom:10px}}
.rias-toggle{{
  display:inline;background:none;border:none;padding:0;cursor:pointer;
  color:var(--acc);font-size:11px;font-weight:500;font-family:inherit;margin-bottom:8px;
}}
.card-foot{{
  display:flex;justify-content:space-between;align-items:center;
  margin-top:auto;padding-top:10px;border-top:0.5px solid var(--border-soft);
}}
.card-data{{font-size:11px;color:var(--hint)}}
.card-link{{
  font-size:11px;color:var(--acc);text-decoration:none;font-weight:500;
  display:inline-flex;align-items:center;gap:3px;
}}
.empty{{
  text-align:center;padding:36px 20px;color:var(--hint);font-size:14px;
  background:var(--surface);border:0.5px solid var(--border);border-radius:var(--radius);
  margin-bottom:44px;
}}
.empty i{{font-size:28px;display:block;margin-bottom:10px;opacity:.5}}
.cal-hdr{{margin-bottom:0}}
.cal-card{{background:var(--surface);border:0.5px solid var(--border);border-radius:var(--radius);padding:26px;margin-bottom:16px}}
.cal-nav{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}}
.cal-nav button{{
  background:none;border:0.5px solid rgba(0,0,0,.1);border-radius:8px;
  padding:8px 14px;cursor:pointer;color:var(--muted);font-family:inherit;
  line-height:1;display:inline-flex;align-items:center;
}}
.cal-nav button:hover{{border-color:var(--active);color:var(--active)}}
.cal-month{{font-size:15px;font-weight:500;color:var(--text);letter-spacing:-.01em}}
.cal-grid{{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-bottom:8px}}
.ch{{
  font-size:10px;text-align:center;color:var(--hint);font-weight:700;
  letter-spacing:.05em;text-transform:uppercase;padding:4px 0;
}}
.cd{{
  position:relative;text-align:center;padding:10px 2px;border-radius:8px;
  font-size:13px;line-height:1;user-select:none;color:var(--hint);
}}
.cd.oggi{{outline:1.5px solid rgba(0,0,0,.22);outline-offset:-1px;color:var(--text);font-weight:600}}
.cd.ha-atti{{
  background:var(--acc-bg);color:var(--acc);font-weight:600;cursor:pointer;
}}
.cd.ha-atti:hover{{filter:brightness(.97)}}
.cd.selected{{background:var(--active)!important;color:#fff!important;outline:none}}
.cd.vuoto{{pointer-events:none}}
.dot{{
  position:absolute;bottom:3px;left:50%;transform:translateX(-50%);
  font-size:8px;font-weight:700;line-height:1;color:inherit;
}}
.cal-legend{{display:flex;gap:20px;margin-top:18px;padding-top:14px;border-top:0.5px solid rgba(0,0,0,.07);flex-wrap:wrap}}
.cal-legend-item{{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--hint)}}
.cal-legend-sw{{width:12px;height:12px;border-radius:3px;flex-shrink:0}}
.cal-legend-sw.atti{{background:var(--acc-bg);border:1px solid var(--acc-border)}}
.cal-legend-sw.oggi{{border:1.5px solid rgba(0,0,0,.22)}}
.cal-legend-sw.sel{{background:var(--active)}}
#pannello{{
  display:none;background:var(--surface);
  border:0.5px solid var(--border);border-radius:var(--radius);padding:24px;
}}
#pannello-hdr{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}}
#pannello-eyebrow{{
  font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  color:var(--hint);margin-bottom:5px;
}}
#pannello-hdr h4{{font-size:17px;font-weight:400;color:var(--text);letter-spacing:-.01em}}
#pannello-close{{
  background:none;border:0.5px solid rgba(0,0,0,.13);cursor:pointer;font-size:12px;
  color:var(--muted);padding:7px 14px;border-radius:8px;font-family:inherit;
  font-weight:500;flex-shrink:0;margin-top:2px;
}}
#pannello-body{{display:flex;flex-direction:column;gap:10px}}
#rias-ov{{
  display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;
  align-items:center;justify-content:center;padding:20px;
  backdrop-filter:blur(3px);
}}
#rias-ov.open{{display:flex}}
#rias-box{{
  background:#fff;border-radius:16px;max-width:620px;width:100%;
  max-height:88vh;overflow-y:auto;padding:28px;position:relative;
  box-shadow:0 24px 60px rgba(0,0,0,.3);
}}
#rias-chip{{
  display:inline-flex;font-size:11px;font-weight:600;padding:4px 12px;
  border-radius:100px;background:var(--acc-bg);color:var(--acc);margin-bottom:12px;
}}
#rias-ogg{{font-size:15px;font-weight:700;line-height:1.4;margin-bottom:14px}}
#rias-txt{{font-size:13.5px;line-height:1.85;color:#444;margin-bottom:20px}}
#rias-ft{{
  font-size:11px;color:#aaa;border-top:1px solid #f0f0f0;padding-top:14px;
  display:flex;justify-content:space-between;align-items:center;
}}
#rias-lnk{{font-size:12px;font-weight:600;color:var(--acc);text-decoration:none}}
#rias-x{{
  position:absolute;top:14px;right:14px;width:28px;height:28px;border:none;
  background:#f0f0f0;border-radius:50%;cursor:pointer;font-size:15px;color:#555;
}}
footer{{
  text-align:center;padding:32px 20px 24px;font-size:12.5px;color:var(--muted);
  border-top:1px solid var(--border);max-width:920px;margin:0 auto;
}}
footer a{{color:#555350;text-decoration:none}}
footer a:hover{{text-decoration:underline}}
.newsletter{{
  margin:24px auto 20px;max-width:440px;text-align:center;
}}
.newsletter p{{
  font-size:12px;font-weight:600;color:#555;margin:0 0 10px;
  text-transform:uppercase;letter-spacing:.07em;
}}
.newsletter form{{display:flex;gap:8px;justify-content:center;flex-wrap:wrap}}
.newsletter input[type=email]{{
  font-size:13px;padding:8px 12px;border:1px solid rgba(0,0,0,.15);
  border-radius:8px;outline:none;font-family:inherit;flex:1;
  min-width:160px;max-width:220px;
}}
.newsletter input[type=submit]{{
  font-size:13px;font-weight:600;padding:8px 16px;background:var(--header);
  color:#fff;border:none;border-radius:8px;cursor:pointer;font-family:inherit;
}}
@media(max-width:640px){{
  header{{padding:36px 16px 32px}}
  header h1{{font-size:34px}}
  .stats{{display:flex;overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%;max-width:100%}}
  .stat{{padding:14px 20px;min-width:72px;flex-shrink:0}}
  .stat-n{{font-size:26px}}
  main{{padding:24px 14px 60px}}
  .grid{{grid-template-columns:1fr}}
  .cal-card{{padding:18px 14px}}
  #pannello{{padding:18px 14px}}
  #rias-box{{padding:20px 16px;border-radius:12px}}
}}
</style>
</head>
<body>

<header>
  <p class="cap">Monitoraggio civico indipendente</p>
  <h1>Albo in chiaro</h1>
  <p class="sub">Pieve Emanuele</p>
  <p class="desc">Ogni atto pubblicato sull'albo pretorio del Comune, letto automaticamente e spiegato in parole semplici. Progetto civico indipendente, non affiliato al Comune di Pieve Emanuele.</p>
  <div class="stats">
    <div class="stat"><div class="stat-n">{n_tot}</div><div class="stat-l">atti in archivio</div></div>
    <div class="stat"><div class="stat-n">{n_mese}</div><div class="stat-l">questo mese</div></div>
    <div class="stat"><div class="stat-n">{n_oggi}</div><div class="stat-l">pubblicati oggi</div></div>
  </div>
  <p style="font-size:11px;color:rgba(255,255,255,.5);margin-top:16px">Aggiornato {data_agg}</p>
</header>

<main>
  <div class="chips" id="chips">{filtri_html}</div>

  <div class="toolbar">
    <i class="ti ti-search"></i>
    <input type="search" id="q" placeholder="Cerca tra gli atti per parola chiave…" autocomplete="off">
  </div>

  <p class="sec-label" id="sec-label">Ultimi 2 giorni</p>
  <div id="cards"></div>

  <p class="sec-label" id="cal-label">Calendario</p>
  <div class="cal-card">
    <div class="cal-nav">
      <button id="cal-prev" aria-label="Mese precedente" type="button"><i class="ti ti-chevron-left"></i></button>
      <div class="cal-month" id="cal-month"></div>
      <button id="cal-next" aria-label="Mese successivo" type="button"><i class="ti ti-chevron-right"></i></button>
    </div>
    <div class="cal-grid" id="cal-weekdays"></div>
    <div class="cal-grid" id="cal-grid"></div>
    <div class="cal-legend">
      <div class="cal-legend-item"><div class="cal-legend-sw atti"></div>Giorni con atti — clicca per la vista giornaliera</div>
      <div class="cal-legend-item"><div class="cal-legend-sw oggi"></div>Oggi</div>
      <div class="cal-legend-item"><div class="cal-legend-sw sel"></div>Giorno selezionato</div>
    </div>
  </div>
  <div id="pannello">
    <div id="pannello-hdr">
      <div>
        <div id="pannello-eyebrow">Vista giornaliera</div>
        <h4 id="pannello-titolo"></h4>
      </div>
      <button id="pannello-close" aria-label="Chiudi">✕ Chiudi</button>
    </div>
    <div id="pannello-body"></div>
  </div>
</main>

<footer>
  <div style="margin:0 auto 14px;max-width:660px;line-height:1.75">
    <strong>Come funziona.</strong> Un automatismo legge ogni giorno l'Albo Pretorio del Comune di Pieve Emanuele, scarica gli atti e li riassume con l'intelligenza artificiale. L'estrazione automatica può contenere errori: fa fede sempre l'atto originale, linkato in ogni scheda.
  </div>
  <div class="newsletter">
    <p>Ricevi gli atti nella tua email</p>
    <form action="{NEWSLETTER_EMBED}" method="post">
      <input type="email" name="email" placeholder="la tua email" required>
      <input type="submit" value="Iscriviti">
    </form>
  </div>
  Dati: <a href="https://pieveemanuele.trasparenza-valutazione-merito.it/web/trasparenza" target="_blank" rel="noopener">Amministrazione Trasparente — Comune di Pieve Emanuele</a><br>
  · <a href="https://github.com/NT0wers84/albo-pretorio" target="_blank" rel="noopener">Codice su GitHub</a><br>
  · <a href="{FEED_URL}">Feed RSS</a><br>
  · Progetto gemello: <a href="https://nt0wers84.github.io/bilanciopertutti/" target="_blank" rel="noopener">OpenSpese Pieve Emanuele</a>
</footer>

<div id="rias-ov">
  <div id="rias-box">
    <button id="rias-x" aria-label="Chiudi">&#x2715;</button>
    <div id="rias-chip"></div>
    <p id="rias-ogg"></p>
    <p id="rias-txt"></p>
    <div id="rias-ft">
      <span id="rias-dt"></span>
      <a id="rias-lnk" href="#" target="_blank" rel="noopener">Leggi atto completo &#x2192;</a>
    </div>
  </div>
</div>

<script>
const ALL_ATTI = {all_atti_js};
const ATTI_COUNTS = {atti_counts_js};

const PILL = {{
  delibera:['pill-acc','ti-building-bank'],
  determinazione:['pill-suc','ti-clipboard-list'],
  ordinanza:['pill-dan','ti-alert-triangle'],
  avviso:['pill-war','ti-speakerphone'],
  bando:['pill-pro','ti-file-text'],
  appalto:['pill-pro','ti-hammer'],
  'variazione-bilancio':['pill-acc','ti-chart-bar'],
}};

let filtroTipo = '';
let query = '';

function pillFor(tn) {{
  for (const [k,v] of Object.entries(PILL)) {{
    if ((tn||'').toLowerCase().includes(k)) return v;
  }}
  return ['pill-neu','ti-file'];
}}

function cutoffDk() {{
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}}

function attiFiltrati() {{
  let list = ALL_ATTI;
  if (filtroTipo) list = list.filter(a => (a.tipoNorm||'').includes(filtroTipo));
  const q = query.trim().toLowerCase();
  if (q) {{
    return list.filter(a =>
      (a.oggetto||'').toLowerCase().includes(q) ||
      (a.riassunto||'').toLowerCase().includes(q)
    );
  }}
  const cut = cutoffDk();
  return list.filter(a => a.dk && a.dk >= cut);
}}

function cardHTML(a, variant) {{
  const [cls, icon] = pillFor(a.tipoNorm);
  const rias = a.riassunto || '';
  const short = rias.length > 165 ? rias.slice(0, 165) + '…' : rias;
  const hasMore = rias.length > 165;
  const link = a.url
    ? `<a href="${{a.url}}" target="_blank" rel="noopener" class="card-link">Leggi <i class="ti ti-arrow-right"></i></a>`
    : '';
  const riasBlock = rias
    ? `<p class="card-rias">${{short}}</p>${{hasMore ? `<button class="rias-toggle" data-idx="${{a.idx}}">leggi tutto</button>` : ''}}`
    : '';
  const cardCls = variant === 'panel' ? 'card panel-card' : 'card';
  return `<div class="${{cardCls}}">
    <div class="pill ${{cls}}"><i class="ti ${{icon}}"></i>${{a.tipo}}</div>
    <h4 class="card-title">${{a.oggetto}}</h4>
    ${{riasBlock}}
    <div class="card-foot">
      <span class="card-data">${{a.data}} · ${{a.numero}}</span>
      ${{link}}
    </div>
  </div>`;
}}

function renderCards() {{
  const list = attiFiltrati();
  const el = document.getElementById('cards');
  const lbl = document.getElementById('sec-label');
  if (query.trim()) {{
    lbl.textContent = list.length ? `Ricerca “${{query.trim()}}” · ${{list.length}} risultati` : `Ricerca “${{query.trim()}}” · nessun risultato`;
  }} else {{
    lbl.textContent = `Ultimi 2 giorni · ${{list.length}} atti`;
  }}
  el.innerHTML = list.length
    ? `<div class="grid">${{list.map(a => cardHTML(a)).join('')}}</div>`
    : `<p class="empty"><i class="ti ti-search-off"></i>Nessun atto trovato. Prova la ricerca per esplorare l'archivio.</p>`;
}}

let selectedDay = null;

function apriGiorno(dk) {{
  if (selectedDay === dk) {{
    selectedDay = null;
    document.getElementById('pannello').style.display = 'none';
    renderCalendar();
    return;
  }}
  const lista = ALL_ATTI.filter(a => a.dk === dk);
  if (!lista.length) return;
  selectedDay = dk;
  const [,m,d] = dk.split('-');
  document.getElementById('pannello-titolo').textContent =
    `${{parseInt(d,10)}}/${{m}}/${{dk.slice(0,4)}} · ${{lista.length}} atti`;
  document.getElementById('pannello-body').innerHTML = lista.map(a => cardHTML(a, 'panel')).join('');
  document.getElementById('pannello').style.display = 'block';
  renderCalendar();
}}

function chiudiPannello() {{
  selectedDay = null;
  document.getElementById('pannello').style.display = 'none';
  renderCalendar();
}}

function openModal(idx) {{
  const a = ALL_ATTI[idx];
  if (!a) return;
  document.getElementById('rias-chip').textContent = a.tipo || '';
  document.getElementById('rias-ogg').textContent = a.oggetto || '';
  document.getElementById('rias-txt').textContent = a.riassunto || '';
  document.getElementById('rias-dt').textContent = (a.data||'') + ' · ' + (a.numero||'');
  document.getElementById('rias-lnk').href = a.url || '#';
  document.getElementById('rias-ov').classList.add('open');
  document.body.style.overflow = 'hidden';
}}

function closeModal() {{
  document.getElementById('rias-ov').classList.remove('open');
  document.body.style.overflow = '';
}}

document.getElementById('q').addEventListener('input', e => {{ query = e.target.value; renderCards(); }});
document.getElementById('chips').addEventListener('click', e => {{
  const btn = e.target.closest('.chip');
  if (!btn) return;
  document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
  btn.classList.add('active');
  filtroTipo = btn.dataset.tipo || '';
  renderCards();
}});
document.getElementById('pannello-close').addEventListener('click', chiudiPannello);
document.getElementById('rias-x').addEventListener('click', closeModal);
document.getElementById('rias-ov').addEventListener('click', e => {{ if (e.target.id === 'rias-ov') closeModal(); }});
document.addEventListener('keydown', e => {{ if (e.key === 'Escape') closeModal(); }});
document.addEventListener('click', e => {{
  const btn = e.target.closest('.rias-toggle');
  if (btn) openModal(parseInt(btn.dataset.idx, 10));
}});

const MESI = ['Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno','Luglio',
  'Agosto','Settembre','Ottobre','Novembre','Dicembre'];
const GIORNI_SETT = ['Lun','Mar','Mer','Gio','Ven','Sab','Dom'];
const oggiReale = new Date();
let calYear = oggiReale.getFullYear();
let calMonth = oggiReale.getMonth();

document.getElementById('cal-weekdays').innerHTML =
  GIORNI_SETT.map(g => `<div class="ch">${{g}}</div>`).join('');

function pad2(n) {{ return String(n).padStart(2, '0'); }}

function renderCalendar() {{
  const nomeMese = MESI[calMonth].charAt(0).toUpperCase() + MESI[calMonth].slice(1);
  document.getElementById('cal-label').textContent = `Calendario · ${{nomeMese}} ${{calYear}}`;
  document.getElementById('cal-month').textContent = `${{nomeMese}} ${{calYear}}`;

  const primoGiorno = new Date(calYear, calMonth, 1).getDay();
  const offset = primoGiorno === 0 ? 6 : primoGiorno - 1;
  const giorniMese = new Date(calYear, calMonth + 1, 0).getDate();

  let html = '';
  for (let i = 0; i < offset; i++) html += '<div class="cd vuoto"></div>';
  for (let d = 1; d <= giorniMese; d++) {{
    const dk = `${{calYear}}-${{pad2(calMonth + 1)}}-${{pad2(d)}}`;
    const n = ATTI_COUNTS[dk] || 0;
    let cls = 'cd';
    const isOggi = calYear === oggiReale.getFullYear() && calMonth === oggiReale.getMonth() && d === oggiReale.getDate();
    const isSelected = selectedDay === dk;
    if (isOggi && !isSelected) cls += ' oggi';
    if (isSelected) cls += ' selected';
    if (n > 0) {{
      cls += ' ha-atti';
      html += `<div class="${{cls}}" data-dk="${{dk}}">${{d}}<span class="dot">${{n}}</span></div>`;
    }} else {{
      html += `<div class="${{cls}}">${{d}}</div>`;
    }}
  }}
  document.getElementById('cal-grid').innerHTML = html;
  document.querySelectorAll('.cd.ha-atti').forEach(el => {{
    el.addEventListener('click', () => apriGiorno(el.dataset.dk));
  }});
}}

document.getElementById('cal-prev').addEventListener('click', () => {{
  calMonth--;
  if (calMonth < 0) {{ calMonth = 11; calYear--; }}
  selectedDay = null;
  document.getElementById('pannello').style.display = 'none';
  renderCalendar();
}});
document.getElementById('cal-next').addEventListener('click', () => {{
  calMonth++;
  if (calMonth > 11) {{ calMonth = 0; calYear++; }}
  selectedDay = null;
  document.getElementById('pannello').style.display = 'none';
  renderCalendar();
}});

document.querySelectorAll('.chip').forEach(ch => {{
  const tn = ch.dataset.tipo;
  const n = tn ? ALL_ATTI.filter(a => (a.tipoNorm||'').includes(tn)).length : ALL_ATTI.length;
  ch.textContent = ch.textContent + ' · ' + n;
}});

renderCards();
renderCalendar();
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
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"Sito generato: {OUTPUT} ({len(content) // 1024} KB)")


if __name__ == "__main__":
    main()
