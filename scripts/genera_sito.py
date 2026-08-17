"""
genera_sito.py — Genera docs/index.html dall'archivio atti.
Sostituisce aggiorna_standalone.py: niente patch su bundle, HTML rigenerato ogni run.
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
    nome_mese = datetime(anno, mese, 1).strftime("%B %Y").capitalize()

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

    cal_matrix = calendar.monthcalendar(anno, mese)
    giorni_hdr = "".join(f'<div class="ch">{g}</div>' for g in ["Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom"])

    celle = ""
    for settimana in cal_matrix:
        for g in settimana:
            if g == 0:
                celle += '<div class="cd vuoto"></div>'
                continue
            dk = f"{anno}-{mese:02d}-{g:02d}"
            n = atti_counts.get(dk, 0)
            cls = "cd"
            if g == oggi.day:
                cls += " oggi"
            if n > 0:
                cls += " ha-atti"
                celle += f'<div class="{cls}" data-dk="{dk}">{g}<span class="dot">{n}</span></div>'
            else:
                celle += f'<div class="{cls}">{g}</div>'

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
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@latest/tabler-icons.min.css">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --bg:#E8E6E1;--surface:#fff;--border:rgba(0,0,0,.10);
  --text:#141412;--muted:#636158;--hint:#888;
  --acc:#1B4FCA;--header:#123785;
  --acc-bg:#EEF2FF;--dan-bg:#fde8e8;--dan-fg:#b91c1c;
  --suc-bg:#e6f4ea;--suc-fg:#166534;--war-bg:#fef3cd;--war-fg:#92400e;
  --pro-bg:#f0ebfe;--pro-fg:#5b21b6;--radius:12px;
}}
body{{
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
  background:var(--bg);color:var(--text);font-size:14px;line-height:1.5;
  -webkit-font-smoothing:antialiased;
}}
header{{
  background:var(--header);color:#fff;text-align:center;
  padding:48px 20px 40px;
}}
header .cap{{
  font-size:10px;letter-spacing:.14em;text-transform:uppercase;
  color:rgba(255,255,255,.65);margin-bottom:10px;
}}
header h1{{font-size:42px;font-weight:700;letter-spacing:-.03em;margin-bottom:6px}}
header .sub{{font-size:15px;color:rgba(255,255,255,.85);margin-bottom:14px}}
header .desc{{
  font-size:13px;color:rgba(255,255,255,.75);max-width:560px;
  margin:0 auto 28px;line-height:1.7;
}}
.stats{{
  display:inline-flex;border-radius:14px;overflow:hidden;
  border:1px solid rgba(255,255,255,.15);
}}
.stat{{padding:16px 28px;background:rgba(255,255,255,.08);min-width:90px}}
.stat+.stat{{border-left:1px solid rgba(255,255,255,.12)}}
.stat-n{{font-size:34px;font-weight:400;line-height:1;margin-bottom:7px;letter-spacing:-.02em}}
.stat-l{{
  font-size:10px;text-transform:uppercase;letter-spacing:.09em;
  color:rgba(255,255,255,.9);font-weight:600;
}}
main{{max-width:860px;margin:0 auto;padding:28px 16px 64px}}
.toolbar{{margin-bottom:22px}}
.toolbar input{{
  width:100%;padding:11px 14px;font-size:14px;border:1px solid var(--border);
  border-radius:10px;background:var(--surface);font-family:inherit;
  outline:none;color:var(--text);
}}
.toolbar input:focus{{border-color:var(--acc)}}
.chips{{display:flex;flex-wrap:wrap;gap:6px;margin:14px 0 6px}}
.chip{{
  font-size:12px;padding:6px 14px;border-radius:100px;cursor:pointer;
  border:1px solid var(--border);background:var(--surface);color:var(--muted);
  font-family:inherit;transition:all .12s;
}}
.chip:hover{{border-color:rgba(0,0,0,.2)}}
.chip.active{{background:var(--acc-bg);color:var(--acc);border-color:var(--acc);font-weight:600}}
.sec-label{{
  font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;
  color:var(--hint);margin-bottom:12px;
}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px;margin-bottom:32px}}
.card{{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px 18px;display:flex;flex-direction:column;
}}
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
.card-title{{font-size:14px;font-weight:600;line-height:1.45;margin-bottom:8px;flex:1}}
.card-rias{{font-size:12px;color:var(--muted);line-height:1.7;margin-bottom:10px}}
.rias-toggle{{
  display:inline;background:none;border:none;padding:0;cursor:pointer;
  color:var(--acc);font-size:11px;font-weight:500;font-family:inherit;margin-bottom:8px;
}}
.card-foot{{
  display:flex;justify-content:space-between;align-items:center;
  margin-top:auto;padding-top:10px;border-top:1px solid var(--border);
}}
.card-data{{font-size:11px;color:var(--hint)}}
.card-link{{
  font-size:11px;color:var(--acc);text-decoration:none;font-weight:500;
  display:inline-flex;align-items:center;gap:3px;
}}
.empty{{font-size:13px;color:var(--hint);padding:24px 0;font-style:italic;text-align:center}}
.cal-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px}}
.cal-grid{{display:grid;grid-template-columns:repeat(7,1fr);gap:4px}}
.ch{{font-size:11px;color:var(--hint);text-align:center;padding:6px 0;font-weight:600}}
.cd{{
  font-size:13px;text-align:center;padding:10px 2px;border-radius:8px;
  color:var(--muted);position:relative;user-select:none;
}}
.cd.ha-atti{{color:var(--acc);font-weight:600;cursor:pointer;background:var(--acc-bg)}}
.cd.ha-atti:hover{{opacity:.85}}
.cd.oggi{{outline:2px solid var(--acc);color:var(--text);font-weight:600}}
.cd.vuoto{{pointer-events:none}}
.dot{{
  position:absolute;bottom:2px;left:50%;transform:translateX(-50%);
  font-size:9px;color:var(--acc);font-weight:700;
}}
#pannello{{
  display:none;margin-top:16px;background:var(--surface);
  border:1px solid var(--border);border-radius:var(--radius);padding:18px;
}}
#pannello-hdr{{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px}}
#pannello-hdr h4{{font-size:15px;font-weight:600}}
#pannello-close{{
  background:none;border:none;cursor:pointer;font-size:18px;
  color:var(--hint);padding:0 4px;
}}
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
  border-top:1px solid var(--border);max-width:860px;margin:0 auto;
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
  header h1{{font-size:32px}}
  .stats{{display:flex;overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%;max-width:100%}}
  .stat{{padding:14px 20px;min-width:72px;flex-shrink:0}}
  .stat-n{{font-size:26px}}
  main{{padding:24px 14px 60px}}
  .grid{{grid-template-columns:1fr}}
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
  <div class="toolbar">
    <input type="search" id="q" placeholder="Cerca per oggetto o riassunto…" autocomplete="off">
    <div class="chips" id="chips">{filtri_html}</div>
    <p class="sec-label" id="sec-label">Ultimi 2 giorni</p>
  </div>
  <div class="grid" id="cards"></div>

  <p class="sec-label">Calendario — {nome_mese}</p>
  <div class="cal-wrap"><div class="cal-grid">{giorni_hdr}{celle}</div></div>
  <div id="pannello">
    <div id="pannello-hdr">
      <h4 id="pannello-titolo"></h4>
      <button id="pannello-close" aria-label="Chiudi">&#x2715;</button>
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

function cardHTML(a) {{
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
  return `<div class="card">
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
    lbl.textContent = list.length ? `${{list.length}} risultati` : 'Nessun risultato';
  }} else {{
    lbl.textContent = 'Ultimi 2 giorni';
  }}
  el.innerHTML = list.length ? list.map(cardHTML).join('') : '<p class="empty">Nessun atto in questo periodo. Prova la ricerca per esplorare l\\'archivio.</p>';
}}

function apriGiorno(dk) {{
  const lista = ALL_ATTI.filter(a => a.dk === dk);
  if (!lista.length) return;
  const [,m,d] = dk.split('-');
  document.getElementById('pannello-titolo').textContent =
    `Atti del ${{d}}/${{m}}/${{dk.slice(0,4)}} (${{lista.length}})`;
  document.getElementById('pannello-body').innerHTML = lista.map(cardHTML).join('');
  document.getElementById('pannello').style.display = 'block';
}}

function chiudiPannello() {{
  document.getElementById('pannello').style.display = 'none';
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
document.querySelectorAll('.cd.ha-atti').forEach(el => {{
  el.addEventListener('click', () => apriGiorno(el.dataset.dk));
}});

renderCards();
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
