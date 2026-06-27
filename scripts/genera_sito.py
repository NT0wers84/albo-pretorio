"""
genera_sito.py — Genera il sito statico GitHub Pages dall'archivio degli atti.
Output: docs/index.html (GitHub Pages serve dalla cartella /docs)

Layout (dal disegno):
  - Header: titolo + data aggiornamento
  - Sezione OGGI: card degli atti pubblicati oggi o negli ultimi 7 giorni
  - Sezione CALENDARIO: griglia mensile cliccabile, ogni giorno mostra gli atti
"""

import json
import html
from pathlib import Path
from datetime import date, datetime, timedelta
from collections import defaultdict

ATTI_JSON  = Path("data/atti.json")
DOCS_DIR   = Path("docs")
OUTPUT     = DOCS_DIR / "index.html"

EMOJI_TIPO = {
    "delibera":            "🏛️",
    "determinazione":      "📋",
    "ordinanza":           "⚠️",
    "avviso":              "📢",
    "bando":               "📣",
    "appalto":             "🔨",
    "variazione-bilancio": "💰",
}

COLORE_TIPO = {
    "delibera":            "#1a6fc4",
    "determinazione":      "#2e7d32",
    "ordinanza":           "#c62828",
    "avviso":              "#6a1b9a",
    "bando":               "#e65100",
    "appalto":             "#4e342e",
    "variazione-bilancio": "#1565c0",
}


def emoji_tipo(tipo_norm: str) -> str:
    for k, v in EMOJI_TIPO.items():
        if k in (tipo_norm or "").lower():
            return v
    return "📄"


def colore_tipo(tipo_norm: str) -> str:
    for k, v in COLORE_TIPO.items():
        if k in (tipo_norm or "").lower():
            return v
    return "#555"


def tipo_breve(tipo_raw: str) -> str:
    """Prende solo la parte dopo '/' e la mette in Title Case."""
    if "/" in tipo_raw:
        return tipo_raw.split("/")[-1].strip().title()
    return tipo_raw.strip().title()


def card_atto(atto: dict) -> str:
    tipo_norm = atto.get("tipo_norm", "atto")
    em   = emoji_tipo(tipo_norm)
    col  = colore_tipo(tipo_norm)
    tipo = tipo_breve(atto.get("tipo", "Atto"))
    num  = atto.get("numero_raw", "")
    ogg  = html.escape(atto.get("oggetto", "")[:180])
    rias = html.escape(atto.get("riassunto", ""))
    url  = atto.get("url_dettaglio", "#")
    data = atto.get("data_inizio", "")

    return f"""
    <div class="card">
      <div class="card-header" style="background:{col}">
        <span class="tipo-badge">{em} {html.escape(tipo)}</span>
        <span class="numero">{html.escape(num)}</span>
      </div>
      <div class="card-body">
        <p class="oggetto">{ogg}</p>
        {"<p class='riassunto'>" + rias + "</p>" if rias else ""}
      </div>
      <div class="card-footer">
        <span class="data">📅 {html.escape(data)}</span>
        {"<a href='" + html.escape(url) + "' target='_blank' class='link-atto'>🔗 Atto completo</a>" if url and url != "#" else ""}
      </div>
    </div>"""


def genera_html(atti: list[dict]) -> str:
    oggi = date.today()
    oggi_str = oggi.strftime("%d/%m/%Y")
    anno_corrente = oggi.year
    mese_corrente = oggi.month

    # Raggruppa atti per data inizio
    per_data: dict[str, list] = defaultdict(list)
    for a in atti:
        d = a.get("data_inizio", "")
        if d:
            per_data[d].append(a)

    # Atti recenti (ultimi 7 giorni)
    atti_recenti = []
    for i in range(7):
        giorno = (oggi - timedelta(days=i)).isoformat()
        atti_recenti.extend(per_data.get(giorno, []))

    # Atti di oggi
    atti_oggi = per_data.get(oggi.isoformat(), [])

    # Sezione OGGI / RECENTI
    if atti_oggi:
        titolo_recenti = f"Oggi — {oggi_str}"
        atti_da_mostrare = atti_oggi
    elif atti_recenti:
        titolo_recenti = f"Ultimi 7 giorni"
        atti_da_mostrare = atti_recenti[:12]
    else:
        titolo_recenti = "Atti più recenti"
        atti_da_mostrare = atti[:12]

    cards_recenti = "".join(card_atto(a) for a in atti_da_mostrare)
    if not cards_recenti:
        cards_recenti = '<p class="nessun-atto">Nessun atto pubblicato di recente.</p>'

    # Sezione CALENDARIO — mese corrente
    import calendar
    cal = calendar.monthcalendar(anno_corrente, mese_corrente)
    nome_mese = datetime(anno_corrente, mese_corrente, 1).strftime("%B %Y").capitalize()

    giorni_settimana = ["Lun", "Mar", "Mer", "Gio", "Ven", "Sab", "Dom"]
    header_cal = "".join(f'<div class="cal-header-cell">{g}</div>' for g in giorni_settimana)

    celle_cal = ""
    for settimana in cal:
        for giorno in settimana:
            if giorno == 0:
                celle_cal += '<div class="cal-cell vuoto"></div>'
            else:
                data_key = f"{anno_corrente}-{mese_corrente:02d}-{giorno:02d}"
                n_atti = len(per_data.get(data_key, []))
                is_oggi = (giorno == oggi.day and mese_corrente == oggi.month)
                cls = "cal-cell"
                if is_oggi:
                    cls += " oggi"
                if n_atti > 0:
                    cls += " ha-atti"
                badge = f'<span class="badge">{n_atti}</span>' if n_atti > 0 else ""
                celle_cal += f'<div class="{cls}" onclick="mostraGiorno(\'{data_key}\')">{giorno}{badge}</div>'

    # Pannello dettaglio giorno (nascosto, riempito da JS)
    # Prepara dati JSON per JS
    atti_per_js = {}
    for data_key, lista in per_data.items():
        if data_key.startswith(f"{anno_corrente}-{mese_corrente:02d}"):
            atti_per_js[data_key] = [
                {
                    "tipo": tipo_breve(a.get("tipo", "Atto")),
                    "tipo_norm": a.get("tipo_norm", ""),
                    "numero": a.get("numero_raw", ""),
                    "oggetto": a.get("oggetto", "")[:200],
                    "riassunto": a.get("riassunto", ""),
                    "url": a.get("url_dettaglio", ""),
                    "data": a.get("data_inizio", ""),
                }
                for a in lista
            ]

    atti_json_str = json.dumps(atti_per_js, ensure_ascii=False)

    # Statistiche
    n_tot = len(atti)
    n_mese = sum(
        1 for a in atti
        if (a.get("data_inizio") or "").startswith(f"{anno_corrente}-{mese_corrente:02d}")
    )
    data_agg = datetime.now().strftime("%d/%m/%Y %H:%M")

    return f"""<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Albo Pretorio — Comune di Pieve Emanuele</title>
  <style>
    :root {{
      --blu:    #1a3a5c;
      --azzurro:#1a6fc4;
      --grigio: #f5f7fa;
      --bordo:  #dde3ec;
      --testo:  #1c2b3a;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--grigio);
      color: var(--testo);
    }}

    /* ── HEADER ── */
    header {{
      background: var(--blu);
      color: white;
      padding: 24px 20px 20px;
      text-align: center;
    }}
    header h1 {{ font-size: 1.5rem; font-weight: 700; letter-spacing: .5px; }}
    header h2 {{ font-size: 1rem; font-weight: 400; opacity: .8; margin-top: 4px; }}
    .aggiornamento {{
      margin-top: 10px;
      font-size: .8rem;
      opacity: .65;
    }}
    .stat-bar {{
      display: flex;
      justify-content: center;
      gap: 24px;
      margin-top: 14px;
    }}
    .stat {{
      background: rgba(255,255,255,.12);
      border-radius: 8px;
      padding: 8px 18px;
      text-align: center;
    }}
    .stat-num {{ font-size: 1.4rem; font-weight: 700; }}
    .stat-label {{ font-size: .75rem; opacity: .8; }}

    /* ── LAYOUT ── */
    main {{ max-width: 900px; margin: 0 auto; padding: 20px 16px 60px; }}
    section {{ margin-bottom: 40px; }}
    h3 {{
      font-size: 1.1rem;
      font-weight: 700;
      color: var(--blu);
      margin-bottom: 14px;
      padding-bottom: 8px;
      border-bottom: 2px solid var(--bordo);
    }}

    /* ── CARDS ── */
    .cards-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: 14px;
    }}
    .card {{
      background: white;
      border-radius: 10px;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }}
    .card-header {{
      padding: 10px 14px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .tipo-badge {{
      color: white;
      font-size: .8rem;
      font-weight: 600;
    }}
    .numero {{
      color: rgba(255,255,255,.8);
      font-size: .75rem;
    }}
    .card-body {{ padding: 12px 14px; flex: 1; }}
    .oggetto {{
      font-size: .88rem;
      font-weight: 600;
      line-height: 1.4;
      color: var(--testo);
    }}
    .riassunto {{
      font-size: .82rem;
      color: #555;
      margin-top: 8px;
      line-height: 1.5;
    }}
    .card-footer {{
      padding: 8px 14px;
      border-top: 1px solid var(--bordo);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .data {{ font-size: .75rem; color: #888; }}
    .link-atto {{
      font-size: .78rem;
      color: var(--azzurro);
      text-decoration: none;
      font-weight: 500;
    }}
    .link-atto:hover {{ text-decoration: underline; }}
    .nessun-atto {{ color: #888; font-style: italic; padding: 12px 0; }}

    /* ── CALENDARIO ── */
    .cal-grid {{
      display: grid;
      grid-template-columns: repeat(7, 1fr);
      gap: 4px;
    }}
    .cal-header-cell {{
      text-align: center;
      font-size: .75rem;
      font-weight: 700;
      color: #888;
      padding: 4px 0;
    }}
    .cal-cell {{
      background: white;
      border: 1px solid var(--bordo);
      border-radius: 6px;
      min-height: 44px;
      padding: 6px;
      font-size: .9rem;
      font-weight: 500;
      position: relative;
      cursor: default;
      user-select: none;
    }}
    .cal-cell.vuoto {{
      background: transparent;
      border-color: transparent;
    }}
    .cal-cell.ha-atti {{
      cursor: pointer;
      background: #e8f0fe;
      border-color: var(--azzurro);
      color: var(--azzurro);
      font-weight: 700;
    }}
    .cal-cell.ha-atti:hover {{ background: #d0e4ff; }}
    .cal-cell.oggi {{
      border: 2px solid var(--blu);
      font-weight: 700;
    }}
    .badge {{
      position: absolute;
      top: 3px; right: 4px;
      background: var(--azzurro);
      color: white;
      font-size: .65rem;
      font-weight: 700;
      border-radius: 50%;
      width: 16px; height: 16px;
      display: flex; align-items: center; justify-content: center;
    }}

    /* ── PANNELLO GIORNO ── */
    #pannello-giorno {{
      display: none;
      margin-top: 16px;
      background: white;
      border-radius: 10px;
      border: 1px solid var(--bordo);
      padding: 16px;
    }}
    #pannello-giorno h4 {{
      font-size: 1rem;
      color: var(--blu);
      margin-bottom: 12px;
    }}
    #pannello-giorno .card {{ margin-bottom: 10px; }}
    #chiudi-pannello {{
      float: right;
      background: none;
      border: none;
      font-size: 1.2rem;
      cursor: pointer;
      color: #888;
    }}

    /* ── FOOTER ── */
    footer {{
      text-align: center;
      padding: 24px;
      font-size: .78rem;
      color: #aaa;
    }}
    footer a {{ color: #888; }}

    @media (max-width: 500px) {{
      .cards-grid {{ grid-template-columns: 1fr; }}
      .stat-bar {{ gap: 12px; }}
      .cal-cell {{ min-height: 36px; font-size: .8rem; }}
    }}
  </style>
</head>
<body>

<header>
  <h1>🏛️ Albo Pretorio</h1>
  <h2>Comune di Pieve Emanuele (MI)</h2>
  <div class="aggiornamento">Ultimo aggiornamento: {data_agg}</div>
  <div class="stat-bar">
    <div class="stat">
      <div class="stat-num">{n_tot}</div>
      <div class="stat-label">atti in archivio</div>
    </div>
    <div class="stat">
      <div class="stat-num">{n_mese}</div>
      <div class="stat-label">questo mese</div>
    </div>
    <div class="stat">
      <div class="stat-num">{len(atti_da_mostrare)}</div>
      <div class="stat-label">recenti</div>
    </div>
  </div>
</header>

<main>

  <!-- SEZIONE OGGI / RECENTI -->
  <section>
    <h3>📌 {titolo_recenti}</h3>
    <div class="cards-grid">
      {cards_recenti}
    </div>
  </section>

  <!-- SEZIONE CALENDARIO -->
  <section>
    <h3>📅 Calendario — {nome_mese}</h3>
    <div class="cal-grid">
      {header_cal}
      {celle_cal}
    </div>
    <div id="pannello-giorno">
      <button id="chiudi-pannello" onclick="chiudiPannello()">✕</button>
      <h4 id="pannello-titolo"></h4>
      <div id="pannello-contenuto"></div>
    </div>
  </section>

</main>

<footer>
  Dati ufficiali dall'<a href="https://pieveemanuele.trasparenza-valutazione-merito.it/web/trasparenza/papca-ap/-/papca/igrid/0/Albo_pretorio/" target="_blank">Albo Pretorio del Comune di Pieve Emanuele</a>.
  Progetto open source: <a href="https://github.com/NT0wers84/albo-pretorio" target="_blank">github.com/NT0wers84/albo-pretorio</a>
</footer>

<script>
const ATTI = {atti_json_str};

function formattaData(isoDate) {{
  if (!isoDate) return '';
  const [y,m,d] = isoDate.split('-');
  return `${{d}}/${{m}}/${{y}}`;
}}

const COLORI = {{
  'delibera': '#1a6fc4', 'determinazione': '#2e7d32',
  'ordinanza': '#c62828', 'avviso': '#6a1b9a',
  'bando': '#e65100', 'appalto': '#4e342e',
  'variazione-bilancio': '#1565c0'
}};
const EMOJI = {{
  'delibera':'🏛️','determinazione':'📋','ordinanza':'⚠️',
  'avviso':'📢','bando':'📣','appalto':'🔨','variazione-bilancio':'💰'
}};

function colore(tipo) {{
  for (const [k,v] of Object.entries(COLORI)) {{
    if (tipo && tipo.toLowerCase().includes(k)) return v;
  }}
  return '#555';
}}
function emoji(tipo) {{
  for (const [k,v] of Object.entries(EMOJI)) {{
    if (tipo && tipo.toLowerCase().includes(k)) return v;
  }}
  return '📄';
}}

function mostraGiorno(dataKey) {{
  const lista = ATTI[dataKey];
  if (!lista || lista.length === 0) return;
  const pannello = document.getElementById('pannello-giorno');
  const titolo   = document.getElementById('pannello-titolo');
  const contenuto = document.getElementById('pannello-contenuto');
  titolo.textContent = `Atti del ${{formattaData(dataKey)}} (${{lista.length}})`;
  contenuto.innerHTML = lista.map(a => `
    <div class="card">
      <div class="card-header" style="background:${{colore(a.tipo_norm)}}">
        <span class="tipo-badge">${{emoji(a.tipo_norm)}} ${{a.tipo}}</span>
        <span class="numero">${{a.numero}}</span>
      </div>
      <div class="card-body">
        <p class="oggetto">${{a.oggetto}}</p>
        ${{a.riassunto ? '<p class="riassunto">' + a.riassunto + '</p>' : ''}}
      </div>
      <div class="card-footer">
        <span class="data">📅 ${{a.data}}</span>
        ${{a.url ? '<a href="' + a.url + '" target="_blank" class="link-atto">🔗 Atto completo</a>' : ''}}
      </div>
    </div>
  `).join('');
  pannello.style.display = 'block';
  pannello.scrollIntoView({{behavior: 'smooth', block: 'start'}});
}}

function chiudiPannello() {{
  document.getElementById('pannello-giorno').style.display = 'none';
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
    html_content = genera_html(atti)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Sito generato: {OUTPUT} ({len(html_content)//1024} KB)")


if __name__ == "__main__":
    main()
