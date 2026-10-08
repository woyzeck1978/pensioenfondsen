"""Bouw het HTML-verslag over de inrichting van de aandelenstrategie, met bronvermelding per claim en een bronregister per fonds.

Leest equity_setup_* en verslagcijfers/<id>.json. Schrijft AANDELENSTRATEGIE_2025.html naast dit script.
Draaien met .venv/bin/python (pandas).
"""
import glob, html, json, os, sqlite3
from collections import Counter
import pandas as pd

S = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.expanduser("~/pensioenfondsen-app")
DB = os.path.join(BASE, "data/processed/pension_funds.db")
UIT = os.path.join(S, "AANDELENSTRATEGIE_2025.html")
con = sqlite3.connect(DB)
s = pd.read_sql("select e.*, f.name, f.aum_euro_bn, f.uitvoerder from equity_setup_2025 e join funds f on f.id=e.fund_id", con)
beh = pd.read_sql("select b.*, f.name as fonds, f.aum_euro_bn from equity_setup_beheerders_2025 b join funds f on f.id=b.fund_id", con)
reg = pd.read_sql("select r.*, f.aum_euro_bn from equity_setup_regio_2025 r join funds f on f.id=r.fund_id", con)
totaal_fondsen, totaal_aum = con.execute("select count(*), sum(aum_euro_bn) from funds where aum_euro_bn is not null").fetchone()
J = {int(os.path.basename(p)[:-5]): json.load(open(p)) for p in glob.glob(os.path.join(S, "verslagcijfers", "*.json"))}

COHORT = [(25, "> 25 mld"), (5, "5–25 mld"), (1, "1–5 mld"), (0, "< 1 mld")]
def cohort(a):
    for g, nm in COHORT:
        if pd.notna(a) and a >= g: return nm
    return "onbekend"
s["cohort"] = s.aum_euro_bn.map(cohort); volgorde = [n for _, n in COHORT]
s["strategisch_totaal"] = s.strategisch_pct.where(s.strategisch_basis == "totale portefeuille")
s["allocatie_beste"] = s.feitelijk_pct.fillna(s.berekend_pct).fillna(s.strategisch_totaal)
n = len(s)
E = html.escape
def pct(a, b): return f"{100 * a / b:.0f}%" if b else "–"
def f1(x): return "" if pd.isna(x) else f"{x:.1f}".replace(".", ",")
def f2(x): return "" if pd.isna(x) else f"{x:.2f}".replace(".", ",")
def f0(x): return "" if pd.isna(x) else f"{x:.0f}"
def mld(x): return f"{x:,.0f}".replace(",", ".")
naam = dict(zip(s.fund_id, s.name))
import re as _re
ALIAS = {126: "SPF"}  # afkorting die niet uit de bestandsnaam volgt
def alias(fid):
    if fid in ALIAS: return ALIAS[fid]
    m = _re.match(r"\d+_(.+?)_2025\.pdf$", os.path.basename(J[fid]["pad"]))
    a = m.group(1).replace("_", " ") if m else ""
    return a if a and len(a) <= 12 and a.lower() not in naam[fid].lower() else ""
def toon(fid):
    a = alias(fid)
    return naam[fid] + (f" ({a})" if a else "")
uitv = dict(zip(s.fund_id, s.uitvoerder))
def bron(fid, blok=None):
    """Voetnoot-link naar het bronregister: fonds, verslag, pagina."""
    d = J[fid]; pg = d[blok]["pagina"] if blok else None
    pdf = os.path.basename(d["pad"])
    t = f"{toon(fid)}, {pdf}" + (f", p. {pg}" if pg else "")
    return f'<a class="bron" href="#bron-{fid}" title="{E(t)}">[{fid}{("·p" + str(pg)) if pg else ""}]</a>'
def cite(fid, blok):
    d = J[fid]; b = d[blok]; q = (b.get("letterlijk") or "")[:260]
    return f'<li><strong>{E(toon(fid))}</strong> <span class="mono">{E(os.path.basename(d["pad"]))}</span>' + \
           (f', p. {b["pagina"]}' if b.get("pagina") else "") + (f': <q>{E(q)}</q>' if q else "") + "</li>"

# ---- aggregaten ------------------------------------------------------------------------------------
g1 = s.groupby("cohort").agg(fondsen=("fund_id", "size"), feitelijk=("allocatie_beste", "median"), strategisch=("strategisch_totaal", "median"),
                             pe=("private_equity_pct", "median")).reindex(volgorde)
org = pd.crosstab(s.cohort, s.organisatie).reindex(volgorde).fillna(0).astype(int)
fid_t = s[s.fiduciair_norm.notna()].groupby("fiduciair_norm").agg(fondsen=("fund_id", "size"), aum=("aum_euro_bn", "sum")).sort_values("fondsen", ascending=False)
bm = beh.groupby("naam_norm").agg(mandaten=("id", "size"), fondsen=("fund_id", "nunique")).sort_values(["fondsen", "mandaten"], ascending=False)
bm["aum"] = [beh[beh.naam_norm == i].drop_duplicates("fund_id").aum_euro_bn.sum() for i in bm.index]
stijl_per = pd.crosstab(beh.naam_norm, beh.stijl).reindex(bm.head(12).index).fillna(0).astype(int)
st = pd.crosstab(s.cohort, s.hoofdstijl).reindex(volgorde).fillna(0).astype(int)
for c in ("passief", "gemengd", "actief", "onbekend"):
    if c not in st.columns: st[c] = 0
pp = s[s.passief_pct.notna()]
bmk = s.benchmarks.dropna().str.split("; ").explode().str.strip()
fam = bmk.str.extract(r"(MSCI|FTSE|STOXX|S&P|Solactive|Bloomberg|Russell)", expand=False).fillna("overig/eigen").value_counts()
k = s[s.beheerkosten_aandelen_pct.notna()]
ks = k.groupby("hoofdstijl").beheerkosten_aandelen_pct.agg(["size", "median", lambda x: x.quantile(.25), lambda x: x.quantile(.75)])
ks.columns = ["fondsen", "mediaan", "q25", "q75"]; ks = ks.reindex([c for c in ("passief", "gemengd", "actief") if c in ks.index])
kc = k.groupby("cohort").beheerkosten_aandelen_pct.agg(["size", "median"]).reindex(volgorde); kc.columns = ["fondsen", "mediaan"]
kt = s[s.vermogensbeheerkosten_totaal_pct.notna()]
instr = s.esg_instrumenten.dropna().str.split("; ").explode().str.strip().value_counts()
sf = s.sfdr_artikel.value_counts().sort_index()
som = reg.groupby("fund_id").pct.sum(); vol = reg[reg.fund_id.isin(som[(som > 90) & (som < 110)].index)]
rg = vol.groupby("regio").agg(fondsen=("fund_id", "nunique"), gem=("pct", "mean"), med=("pct", "median")).sort_values("gem", ascending=False)
em = vol[vol.regio == "Opkomende markten"].pct
v = s[s.valuta_afgedekt_pct.notna()]
ontbreekt = {"aandelenallocatie": int(s.allocatie_beste.isna().sum()), "aandelenbeheerder genoemd": int((s.aantal_beheerders == 0).sum()),
             "beheerkosten aandelen": int(s.beheerkosten_aandelen_pct.isna().sum()), "SFDR-artikel": int(s.sfdr_artikel.isna().sum()),
             "regioverdeling": int((~s.fund_id.isin(reg.fund_id)).sum()), "valuta-afdekking": int(s.valuta_afgedekt_pct.isna().sum())}

# voorbeeldbronnen per onderwerp: grootste fondsen met een gevuld blok
def voorbeelden(mask, blok, k_=4):
    ids = s[mask].sort_values("aum_euro_bn", ascending=False).fund_id.head(k_)
    return "".join(cite(int(i), blok) for i in ids)
def bronlinks(mask, blok, k_=5):
    ids = s[mask].sort_values("aum_euro_bn", ascending=False).fund_id.head(k_)
    return " ".join(bron(int(i), blok) for i in ids)

def tabel(rows, kop, cls=""):
    out = [f'<div class="tw"><table class="{cls}"><thead><tr>' + "".join(f"<th>{E(str(h))}</th>" for h in kop) + "</tr></thead><tbody>"]
    for r in rows:
        out.append("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>")
    return "".join(out) + "</tbody></table></div>"

# ---- bronregister ----------------------------------------------------------------------------------
BLOK = [("allocatie", "Allocatie"), ("beheer", "Beheer"), ("stijl", "Stijl"), ("kosten", "Kosten"), ("esg", "ESG"), ("regio", "Regio"), ("valuta", "Valuta")]
reg_rows = []
for fid in sorted(J, key=lambda i: -(naam_aum := s.set_index("fund_id").aum_euro_bn.get(i, 0) or 0)):
    d = J[fid]; r = s[s.fund_id == fid].iloc[0]
    det = []
    for key, lab in BLOK:
        b = d[key]; q = b.get("letterlijk") or ""
        extra = ""
        if key == "beheer" and b["beheerders"]:
            extra = "<ul>" + "".join(f"<li>{E(m['naam'])}" + (f" — {E(m['product_of_mandaat'])}" if m.get("product_of_mandaat") else "") +
                                     f" <em>({E(m['stijl'])}{(', ' + E(m['segment'])) if m.get('segment') else ''})</em>" + (f" p. {m['pagina']}" if m.get("pagina") else "") + "</li>" for m in b["beheerders"]) + "</ul>"
        if key == "regio" and b["verdeling"]:
            extra = "<p>" + "; ".join(f"{E(w['regio'])} {f1(w['pct'])}%" for w in b["verdeling"]) + f" <em>({E(b['basis'])})</em></p>"
        if key == "esg":
            extra = f"<p>{E('; '.join(b['instrumenten']))}" + (f" · SFDR {b['sfdr_artikel']}" if b.get("sfdr_artikel") else "") + (f" · CO2-doel: {E(b['co2_doel'])}" if b.get("co2_doel") else "") + "</p>"
        if key == "kosten":
            extra = "<p>" + "; ".join(f"{lbl} {f2(b[kk])}%" for kk, lbl in (("beheerkosten_aandelen_pct", "beheer aandelen"), ("transactiekosten_aandelen_pct", "transactie aandelen"), ("vermogensbeheerkosten_totaal_pct", "beheer totaal")) if b.get(kk) is not None) + "</p>"
        if key == "allocatie":
            extra = "<p>" + "; ".join(x for x in (f"feitelijk {f1(b['feitelijk_pct'])}%" if b.get("feitelijk_pct") is not None else "", f"strategisch {f1(b['strategisch_pct'])}% ({E(b['strategisch_basis'])})" if b.get("strategisch_pct") is not None else "", f"private equity {f1(b['private_equity_pct'])}%" if b.get("private_equity_pct") is not None else "") if x) + "</p>"
        if key == "stijl":
            extra = f"<p>{E(b['hoofdstijl'])}" + (f", {f0(b['passief_pct'])}% passief" if b.get("passief_pct") is not None else "") + (f" · benchmarks: {E('; '.join(b['benchmarks']))}" if b.get("benchmarks") else "") + "</p>"
        if key == "valuta" and b.get("aandelen_afgedekt_pct") is not None:
            extra = f"<p>{f0(b['aandelen_afgedekt_pct'])}% afgedekt</p>"
        det.append(f'<div class="blok"><h5>{lab}' + (f' <span class="pg">p. {b["pagina"]}</span>' if b.get("pagina") else "") + f"</h5>{extra}" + (f"<q>{E(q)}</q>" if q else "") + "</div>")
    opm = f'<p class="opm">{E(d["opmerkingen"])}</p>' if d.get("opmerkingen") else ""
    reg_rows.append(f'<details id="bron-{fid}"><summary><span class="fid">{fid}</span> <strong>{E(toon(fid))}</strong> '
                    f'<span class="meta">€ {f1(r.aum_euro_bn)} mld · {E(r.hoofdstijl)}' + (f' · uitvoerder {E(uitv[fid])}' if uitv.get(fid) else '') + f' · <span class="mono">{E(os.path.basename(d["pad"]))}</span></span></summary>'
                    f'<div class="det">{"".join(det)}{opm}</div></details>')

# ---- chartdata ---------------------------------------------------------------------------------------
chart = {
    "stijl": {"labels": volgorde, "series": {c: [int(x) for x in st[c]] for c in ("passief", "gemengd", "actief", "onbekend")}},
    "beheerders": {"labels": list(bm.head(15).index), "fondsen": [int(x) for x in bm.head(15).fondsen], "mandaten": [int(x) for x in bm.head(15).mandaten]},
    "kosten": {c: [round(float(x), 3) for x in k[k.hoofdstijl == c].beheerkosten_aandelen_pct] for c in ks.index},
    "esg": {"labels": list(instr.index), "waarden": [int(x) for x in instr.values], "n": n},
    "regio": {"labels": list(rg.index), "gem": [round(float(x), 1) for x in rg.gem], "fondsen": [int(x) for x in rg.fondsen]},
}

# ---- pagina ---------------------------------------------------------------------------------------------
top3 = bm.loc[[x for x in ("Northern Trust", "BlackRock", "Aegon AM") if x in bm.index]]
P = []
P.append(f"""<title>Aandelenstrategie pensioenfondsen 2025</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Newsreader:opsz,wght@6..72,400;6..72,500;6..72,600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400&display=swap">
<style>
/* Layout: één leeskolom van ~68ch, tabellen en figuren mogen tot 1000px; bronregister onderaan als uitklapbare dossiers. */
:root{{--bg:#f7f7f4;--surface:#ffffff;--ink:#17201f;--ink2:#5a615f;--ink3:#8a918f;--line:#dcdfdb;--accent:#1f5f6b;--accent-soft:#e3eeef;--q:#f1f3ef;
 --c1:#2a78d6;--c2:#eb6834;--c3:#1baf7a;--c4:#eda100;--c5:#8a918f;
 --display:"Newsreader",Georgia,"Times New Roman",serif;--body:"IBM Plex Sans","Helvetica Neue",Arial,sans-serif;--mono:"IBM Plex Mono",Menlo,Consolas,monospace}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#161918;--surface:#1f2322;--ink:#edefec;--ink2:#b6bcb9;--ink3:#7f8683;--line:#343937;--accent:#7cc3cf;--accent-soft:#1d3236;--q:#242928;--c1:#3987e5;--c2:#d95926;--c3:#199e70;--c4:#c98500;--c5:#7f8683;color-scheme:dark}}}}
:root[data-theme="dark"]{{--bg:#161918;--surface:#1f2322;--ink:#edefec;--ink2:#b6bcb9;--ink3:#7f8683;--line:#343937;--accent:#7cc3cf;--accent-soft:#1d3236;--q:#242928;--c1:#3987e5;--c2:#d95926;--c3:#199e70;--c4:#c98500;--c5:#7f8683;color-scheme:dark}}
body{{background:var(--bg);color:var(--ink);font-family:var(--body);font-size:16px;line-height:1.55;margin:0}}
.wrap{{max-width:1000px;margin:0 auto;padding-block:32px 64px;padding-inline:16px}}
header.kop{{border-bottom:1px solid var(--line);padding-bottom:20px;margin-bottom:28px}}
.eyebrow{{font-family:var(--body);text-transform:uppercase;letter-spacing:.08em;font-size:12px;color:var(--accent);font-weight:600}}
h1{{font-family:var(--display);font-weight:500;font-size:clamp(30px,4.5vw,44px);line-height:1.1;margin:8px 0 10px;text-wrap:balance}}
h2{{font-family:var(--display);font-weight:500;font-size:28px;margin:44px 0 12px;text-wrap:balance;scroll-margin-top:16px}}
h3{{font-size:17px;font-weight:600;margin:24px 0 8px}}
h5{{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--ink2);margin:0 0 4px;font-weight:600}}
p,li{{max-width:68ch}}
.lead{{font-size:18px;color:var(--ink2);max-width:68ch}}
.samenvatting li{{margin-bottom:10px}}
.tw{{overflow-x:auto;margin:12px 0 18px}}
table{{border-collapse:collapse;font-size:14px;min-width:100%}}
th,td{{text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);vertical-align:top;font-variant-numeric:tabular-nums}}
th{{font-size:12px;text-transform:uppercase;letter-spacing:.05em;color:var(--ink2);font-weight:600}}
td.num,th.num{{text-align:right}}
.fig{{background:var(--surface);border:1px solid var(--line);border-radius:6px;padding:16px;margin:18px 0}}
.fig h4{{margin:0 0 4px;font-size:15px;font-weight:600}}
.fig .sub{{color:var(--ink2);font-size:13px;margin:0 0 10px}}
.fig canvas{{max-width:100%}}
.bronnen{{background:var(--q);border-left:3px solid var(--accent);padding:10px 14px;margin:14px 0;font-size:14px}}
.bronnen h5{{color:var(--accent)}}
.bronnen ul{{margin:0;padding-left:18px}} .bronnen li{{max-width:none;margin:3px 0}}
q{{quotes:"‘" "’";color:var(--ink2)}}
a.bron{{font-family:var(--mono);font-size:11px;color:var(--accent);text-decoration:none;background:var(--accent-soft);padding:0 4px;border-radius:3px;white-space:nowrap}}
a.bron:hover,a.bron:focus{{outline:2px solid var(--accent)}}
.mono{{font-family:var(--mono);font-size:12.5px;color:var(--ink2)}}
.kpi{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:16px 0}}
.kpi div{{border-top:2px solid var(--accent);padding-top:6px}} .kpi b{{font-family:var(--display);font-size:30px;font-weight:500;display:block;line-height:1.1}}
.kpi span{{font-size:13px;color:var(--ink2)}}
details{{border-bottom:1px solid var(--line);padding:8px 0}}
summary{{cursor:pointer;list-style:none;display:flex;flex-wrap:wrap;gap:8px;align-items:baseline}}
summary::-webkit-details-marker{{display:none}} summary::before{{content:"▸";color:var(--accent);font-size:12px}} details[open] summary::before{{content:"▾"}}
.fid{{font-family:var(--mono);font-size:12px;color:var(--ink3);min-width:28px}} .meta{{font-size:13px;color:var(--ink2)}}
.det{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px;padding:10px 0 6px 20px;font-size:14px}}
.det .blok{{min-width:0}} .det q{{display:block;font-size:13px;margin-top:4px}} .det ul{{margin:4px 0;padding-left:16px}} .det p{{margin:2px 0}}
.pg{{font-family:var(--mono);font-size:11px;color:var(--ink3);text-transform:none;letter-spacing:0}}
.opm{{grid-column:1/-1;color:var(--ink2);font-size:13px;border-top:1px dashed var(--line);padding-top:8px;max-width:none}}
nav.toc{{font-size:14px;display:flex;flex-wrap:wrap;gap:6px 18px;margin:14px 0 0}} nav.toc a{{color:var(--accent);text-decoration:none}}
.filter{{margin:10px 0;font-size:14px}} .filter input{{font:inherit;padding:6px 10px;border:1px solid var(--line);border-radius:4px;background:var(--surface);color:var(--ink);width:min(100%,360px)}}
@media (prefers-reduced-motion:reduce){{*{{animation:none!important;transition:none!important}}}}
</style>
<div class="wrap">
<header class="kop">
<div class="eyebrow">Onderzoeksverslag · jaarverslagen 2025 · {n} fondsen</div>
<h1>Hoe Nederlandse pensioenfondsen hun aandelen beleggen</h1>
<p class="lead">Wie beheert het, in welk product, tegen welke kosten, met welke rol voor ESG, welke regioverdeling en hoeveel daarvan passief. Gelezen uit {n} jaarverslagen over 2025, samen € {mld(s.aum_euro_bn.sum())} miljard van de € {mld(totaal_aum)} miljard bij {totaal_fondsen} fondsen en kringen in de database ({pct(s.aum_euro_bn.sum(), totaal_aum)}).</p>
<nav class="toc"><a href="#samenvatting">Samenvatting</a><a href="#methode">Methode en bronnen</a><a href="#allocatie">1 Allocatie</a><a href="#organisatie">2 Organisatie</a><a href="#partijen">3 Partijen en producten</a><a href="#stijl">4 Passief of actief</a><a href="#kosten">5 Kosten</a><a href="#esg">6 ESG</a><a href="#regio">7 Regio</a><a href="#valuta">8 Valuta</a><a href="#kwaliteit">9 Datakwaliteit</a><a href="#register">Bronregister</a></nav>
</header>
""")
# KPI's
P.append(f"""<div class="kpi">
<div><b>{pct(int((s.hoofdstijl == 'passief').sum()), n)}</b><span>belegt aandelen (vrijwel) volledig passief</span></div>
<div><b>{int(top3.fondsen.sum())}</b><span>van {beh.fund_id.nunique()} fondsen met genoemde beheerder gebruiken Northern Trust, BlackRock of Aegon AM</span></div>
<div><b>{f2(ks.loc['passief','mediaan']) if 'passief' in ks.index else ''}%</b><span>mediane beheerkosten aandelen bij passief; actief {f2(ks.loc['actief','mediaan']) if 'actief' in ks.index else ''}%</span></div>
<div><b>{pct(int(s.co2_doel.notna().sum()), n)}</b><span>heeft een geformuleerd CO2-reductiedoel</span></div>
</div>""")

# Samenvatting
P.append(f"""<h2 id="samenvatting">Samenvatting</h2>
<ul class="samenvatting">
<li><strong>Passief is de norm, actief de uitzondering.</strong> {pct(int((s.hoofdstijl=='passief').sum()), n)} van de fondsen belegt de aandelen (vrijwel) volledig passief, {pct(int((s.hoofdstijl=='gemengd').sum()), n)} combineert een passieve kern met actieve satellieten (meestal opkomende markten of small caps), {pct(int((s.hoofdstijl=='actief').sum()), n)} is overwegend actief. Hoe kleiner het fonds, hoe vaker volledig passief; de grote fondsen zijn vaker gemengd omdat zij eigen maatwerkindices en actieve mandaten voeren. {bronlinks(s.hoofdstijl=='gemengd', 'stijl', 3)}</li>
<li><strong>Drie partijen bedienen twee derde van de genoemde mandaten.</strong> Northern Trust, BlackRock en Aegon AM worden door {int(top3.fondsen.sum())} van de {beh.fund_id.nunique()} fondsen met een genoemde aandelenbeheerder gebruikt, vrijwel altijd via gepoolde indexfondsen (CCF- of FGR-structuren) met een ESG-screening of Paris-aligned variant. Robeco is de grootste enhanced/actieve partij. De fiduciaire laag is nog geconcentreerder: de vijf grootste fiduciairs bedienen {int(fid_t.head(5).fondsen.sum())} van de {int(fid_t.fondsen.sum())} fondsen die er een noemen. {bronlinks(s.fund_id.isin(beh[beh.naam_norm=='Northern Trust'].fund_id), 'beheer', 3)}</li>
<li><strong>Het product is bijna altijd een MSCI-index met ESG-filter.</strong> MSCI komt {int(fam.get('MSCI',0))} keer voor als benchmark, FTSE {int(fam.get('FTSE',0))} keer; {int((s.eigen_index==1).sum())} fondsen, vooral de grote, laten een eigen maatwerkindex bouwen. {bronlinks(s.eigen_index==1, 'esg', 3)}</li>
<li><strong>Passief kost een fractie van actief.</strong> Waar het verslag de kosten per categorie noemt ({len(k)} fondsen) betaalt een passief fonds mediaan {f2(ks.loc['passief','mediaan'])}% van het belegd vermogen in aandelen, een actief fonds {f2(ks.loc['actief','mediaan'])}%. {bronlinks(s.beheerkosten_aandelen_pct.notna(), 'kosten', 3)}</li>
<li><strong>ESG is standaarduitrusting, een klimaatdoel niet.</strong> Uitsluitingen ({pct(int(instr.get('uitsluitingen',0)), n)}), engagement en stembeleid zijn vrijwel universeel; een geformuleerd CO2-reductiedoel heeft {pct(int(s.co2_doel.notna().sum()), n)} van de fondsen, een Paris-aligned benchmark {pct(int(instr.get('Paris-aligned benchmark',0)), n)}. {int((s.sfdr_artikel==8).sum())} fondsen zijn SFDR artikel 8, {int((s.sfdr_artikel==6).sum())} artikel 6; artikel 9 komt niet voor. {bronlinks(s.co2_doel.notna(), 'esg', 3)}</li>
<li><strong>Regionaal volgt men de wereldindex.</strong> Waar een volledige verdeling wordt genoemd ligt Noord-Amerika mediaan rond {f0(rg.loc['Noord-Amerika','med']) if 'Noord-Amerika' in rg.index else '–'}% en opkomende markten rond {f0(em.median())}%; een bewuste afwijking van de marktkapitalisatie is zeldzaam. {bronlinks(s.fund_id.isin(vol.fund_id), 'regio', 3)}</li>
<li><strong>Valutarisico op aandelen wordt meestal half afgedekt.</strong> Mediaan {f0(v.valuta_afgedekt_pct.median())}% bij {len(v)} fondsen; volledig afdekken en niet afdekken komen allebei voor, het laatste met het argument dat de dollar in crises bescherming biedt. {bronlinks(s.valuta_afgedekt_pct==0, 'valuta', 2)}</li>
<li><strong>De aandelenallocatie zelf</strong> ligt mediaan rond {f0(s.allocatie_beste.median())}% van het vermogen en is bij kleine fondsen hoger dan bij grote, waar private equity en andere illiquide categorieën een deel van het risicobudget innemen.</li>
</ul>""")

# Methode
P.append(f"""<h2 id="methode">Methode en bronnen</h2>
<p>De bron is voor elk fonds het door het bestuur vastgestelde <strong>jaarverslag over 2025</strong> (PDF), hetzelfde bestand dat in september 2026 is gebruikt om de kerncijfers te controleren. Kringen van een algemeen pensioenfonds zijn uit het koepelverslag gelezen. De bestandsnamen staan per fonds in het <a href="#register">bronregister</a>.</p>
<ol>
<li><strong>Selectie van pagina's.</strong> Per verslag zijn met trefwoorden ten hoogste 18 pagina's gekozen over de beleggingsmix, de balanspost aandelen, uitbesteding en beheerders, kosten per beleggingscategorie, passief/actief beheer, benchmarks, duurzaamheid, regio's en valuta-afdekking (<span class="mono">aandelen_prep.py</span>).</li>
<li><strong>Uitlezen.</strong> Die pagina's zijn door een taalmodel (Gemini 3.8 Flash, via de Antigravity-CLI) gelezen volgens een vast JSON-schema: per onderwerp het cijfer of de kwalificatie, het PDF-paginanummer en een <em>letterlijk citaat</em> van de vindplaats. De instructie verbood schatten en zelf rekenen (<span class="mono">agy_aandelen.py</span>).</li>
<li><strong>Vastleggen.</strong> De uitlezingen zijn ongewijzigd bewaard (<span class="mono">verslagcijfers/&lt;id&gt;.json</span>) en in drie afgeleide tabellen geladen: <span class="mono">equity_setup_2025</span>, <span class="mono">equity_setup_beheerders_2025</span> en <span class="mono">equity_setup_regio_2025</span>. Beheerdersnamen zijn genormaliseerd (BlackRock, BLK en iShares tellen als één partij).</li>
<li><strong>Controle.</strong> Een steekproef van vijf fondsen (Bakkersbedrijf, HAL, StiPP, SNS REAAL, Zuivel) is regel voor regel tegen de PDF-pagina's gelegd; elk cijfer klopte. Een strategisch gewicht dat alleen binnen de rendementsportefeuille geldt is niet als allocatie geteld.</li>
</ol>
<p>Elke claim in dit verslag verwijst met een code als <a class="bron" href="#register">[106·p14]</a> naar het bronregister: fonds-id en PDF-pagina. Het register toont per fonds het bestand, de pagina en het citaat per onderwerp. Statistieken (medianen, aantallen) zijn uit de volledige set van {n} fondsen berekend; de genoemde voorbeelden zijn de grootste fondsen waarvoor het betreffende gegeven in het verslag staat.</p>
""")

# 1 allocatie
P.append(f"""<h2 id="allocatie">1. Hoeveel aandelen</h2>
{tabel([[c, int(r.fondsen), f1(r.feitelijk), f1(r.strategisch), f1(r.pe)] for c, r in g1.iterrows()], ["cohort (vermogen)", "fondsen", "feitelijk, mediaan %", "strategisch, mediaan %", "private equity, mediaan %"])}
<p>Het feitelijke gewicht komt bij {int(s.feitelijk_pct.notna().sum())} fondsen uit het verslag zelf, bij {int((s.feitelijk_pct.isna() & s.berekend_pct.notna()).sum())} uit de balanspost gedeeld door de totale beleggingen, en bij {int((s.feitelijk_pct.isna() & s.berekend_pct.isna() & s.strategisch_totaal.notna()).sum())} is alleen het strategische gewicht op de totale portefeuille bekend. Bij {int((s.strategisch_basis=='rendementsportefeuille').sum())} fondsen noemt het verslag het aandelengewicht alleen binnen de rendementsportefeuille (bijvoorbeeld 71% bij de kringen van Centraal Beheer APF); dat is geen allocatie van het hele vermogen en is hier buiten beschouwing gelaten. {int(s.private_equity_pct.notna().sum())} fondsen noemen een aparte private-equitypositie; bij {int(s.omvat_private_equity.fillna(0).sum())} zit private equity in de aandelenpost zelf, wat vergelijkingen tussen fondsen vertekent.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.feitelijk_pct.notna(), 'allocatie')}</ul></div>""")

# 2 organisatie
P.append(f"""<h2 id="organisatie">2. Wie beheert het</h2>
<p>Op {int((s.organisatie=='intern').sum())} fondsen na is het vermogensbeheer volledig uitbesteed. De interne organisaties zijn de bekende: APG voor ABP, PGGM voor PFZW, MN voor PMT en PME, en het PGB-bedrijf. Ook daar wordt het aandelenbeheer inmiddels deels extern belegd: ABP implementeerde in 2025 een nieuw extern actief mandaat naast de intern beheerde maatwerkindex {bron(9,'beheer')}, en PFZW bracht de participaties in PGGM-fondsen terug ten gunste van directe marktnoteringen {bron(41,'beheer')}.</p>
{tabel([[c] + [int(org.loc[c, x]) if x in org.columns else 0 for x in ('extern','gemengd','intern')] for c in volgorde], ["cohort", "extern", "gemengd", "intern"])}
<h3>Fiduciair beheerders</h3>
<p>Een fiduciair of integraal beheerder wordt bij {int(s.fiduciair_norm.notna().sum())} van de {n} fondsen genoemd. De markt is sterk geconcentreerd:</p>
{tabel([[E(i), int(r.fondsen), f1(r.aum)] for i, r in fid_t.head(12).iterrows()], ["fiduciair / integraal beheerder", "fondsen", "vermogen van die fondsen, € mld"])}
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.fiduciair_norm.notna(), 'beheer')}</ul></div>""")

# 3 partijen
P.append(f"""<h2 id="partijen">3. Welke partij en welk product</h2>
<p>{len(beh)} aandelenmandaten bij {beh.fund_id.nunique()} fondsen, {beh.naam_norm.nunique()} verschillende partijen. Het vermogen in de tabel is het totale fondsvermogen van de fondsen die de partij noemen, niet de omvang van het mandaat; de verslagen noemen die omvang zelden.</p>
<div class="fig"><h4>Meest genoemde aandelenbeheerders</h4><p class="sub">Aantal fondsen dat de partij als beheerder van (een deel van) de aandelen noemt, met het aantal mandaten als tooltip</p><canvas id="c-beheerders" height="420"></canvas></div>
{tabel([[E(i), int(r.mandaten), int(r.fondsen), f1(r.aum)] for i, r in bm.head(20).iterrows()], ["aandelenbeheerder", "mandaten", "fondsen", "vermogen van die fondsen, € mld"])}
<h3>Stijl per mandaat bij de twaalf meest genoemde partijen</h3>
{tabel([[E(i)] + [int(stijl_per.loc[i, x]) if x in stijl_per.columns else 0 for x in ('passief','enhanced/factor','actief','onbekend')] for i in stijl_per.index], ["beheerder", "passief", "enhanced/factor", "actief", "onbekend"])}
<p>Northern Trust en State Street zijn zuiver passief; BlackRock is overwegend passief met enkele factor- en actieve mandaten; Robeco verkoopt vrijwel uitsluitend kwantitatieve strategieën (Conservative Equity, Enhanced Index); Aegon AM en Goldman Sachs AM voeren zowel index- als actieve fondsen.</p>
<h3>Genoemde producten</h3>
<p>De bouwstenen keren steeds terug: het Northern Trust World SDG Screened Low Carbon Index Fund en het Emerging Markets ESG Leader Index Fund, de BlackRock CCF Developed World ESG Screened en Paris-Aligned Climate Index Funds, het Aegon/MM World Equity Index Fund, de Robeco QI Conservative en Enhanced Index fondsen, en op maat gemaakte MSCI World ESG of Climate Transition mandaten bij State Street en UBS.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{"".join(cite(int(i), 'beheer') for i in beh[beh.product_of_mandaat.notna()].drop_duplicates('fund_id').sort_values('aum_euro_bn', ascending=False).fund_id.head(6))}</ul></div>""")

# 4 stijl
P.append(f"""<h2 id="stijl">4. Passief of actief</h2>
<div class="fig"><h4>Hoofdstijl van de aandelenportefeuille per vermogenscohort</h4><p class="sub">Aantal fondsen; gemengd = passieve kern met actieve satellieten</p><canvas id="c-stijl" height="220"></canvas></div>
{tabel([[c] + [int(st.loc[c, x]) for x in ('passief','gemengd','actief','onbekend')] for c in volgorde], ["cohort", "passief", "gemengd", "actief", "onbekend"])}
<p>{len(pp)} fondsen noemen een percentage passief beheerd: {int((pp.passief_pct>=99).sum())} daarvan volledig, de overige tussen {f0(pp[pp.passief_pct<99].passief_pct.min())}% en {f0(pp[pp.passief_pct<99].passief_pct.max())}%. Hoogovens bijvoorbeeld ging van 27% naar 41% passief in één jaar {bron(106,'stijl')}. Factorbeleggen wordt expliciet toegepast bij {int((s.factorbeleggen==1).sum())} fondsen en expliciet niet bij {int((s.factorbeleggen==0).sum())}. Benchmarkfamilies voor aandelen: {", ".join(f"{E(i)} {int(x)}" for i, x in fam.items())}. Een eigen of maatwerk-ESG-variant van de index hebben {int((s.eigen_index==1).sum())} fondsen; ABP belegt sinds 2024 volgens een index op het eigen ABP-aandelenuniversum {bron(9,'stijl')}, PFZW tegen een customized FTSE All World {bron(41,'stijl')}.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.passief_pct.notna(), 'stijl')}</ul></div>""")

# 5 kosten
P.append(f"""<h2 id="kosten">5. Wat het kost</h2>
<p>Slechts {len(k)} van de {n} verslagen noemen de beheerkosten van de categorie aandelen apart; de meeste rapporteren alleen fondsbrede kosten. Waar het wel staat is het beeld eenduidig.</p>
<div class="fig"><h4>Beheerkosten van de aandelencategorie, per hoofdstijl</h4><p class="sub">Elke stip is een fonds; % van het belegd vermogen in aandelen, 2025</p><canvas id="c-kosten" height="240"></canvas></div>
{tabel([[E(i), int(r.fondsen), f2(r.mediaan), f2(r.q25), f2(r.q75)] for i, r in ks.iterrows()], ["hoofdstijl", "fondsen", "mediaan %", "25e perc.", "75e perc."])}
{tabel([[c, int(r.fondsen) if pd.notna(r.fondsen) else 0, f2(r.mediaan)] for c, r in kc.iterrows()], ["cohort", "fondsen", "mediaan %"])}
<p>Fondsbrede vermogensbeheerkosten over alle categorieën staan bij {len(kt)} fondsen (mediaan {f2(kt.vermogensbeheerkosten_totaal_pct.median())}%). Transactiekosten op aandelen worden bij {int(s.transactiekosten_aandelen_pct.notna().sum())} fondsen genoemd (mediaan {f2(s.transactiekosten_aandelen_pct.median())}%); een prestatievergoeding op aandelen bij {int(s.prestatievergoeding_aandelen_eur_mln.notna().sum())}, meestal nul of klein. De grote fondsen betalen het meest per euro aandelen omdat zij actieve en geconcentreerde strategieën naast hun indexkern voeren: ABP 0,12% {bron(9,'kosten')}, PFZW 0,10% {bron(41,'kosten')}, tegen 0,06% bij StiPP met drie Northern Trust-indexfondsen {bron(31,'kosten')}.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.beheerkosten_aandelen_pct.notna(), 'kosten', 5)}</ul></div>""")

# 6 esg
P.append(f"""<h2 id="esg">6. De rol van ESG</h2>
<div class="fig"><h4>ESG-instrumenten op de aandelenportefeuille</h4><p class="sub">Aantal fondsen dat het instrument in het jaarverslag noemt</p><canvas id="c-esg" height="300"></canvas></div>
<p>SFDR-classificatie: {", ".join(f"artikel {int(i)} bij {int(x)} fondsen" for i, x in sf.items())}, niet genoemd bij {int(s.sfdr_artikel.isna().sum())}. Een aantal kleinere ondernemingsfondsen gebruikt bewust de opt-out en rapporteert als artikel 6, Hoogovens bijvoorbeeld {bron(106,'esg')}. Een CO2-reductiedoel is geformuleerd bij {int(s.co2_doel.notna().sum())} fondsen; het dominante patroon is min 50% in 2030 ten opzichte van 2019 en netto nul in 2050, zoals bij ABP {bron(9,'esg')}, PME {bron(58,'esg')} en Rail &amp; OV {bron(33,'esg')}. Paris-aligned benchmarks zijn de jongste stap: {int(instr.get('Paris-aligned benchmark',0))} fondsen noemen er een, meestal via het BlackRock of L&amp;G Paris-Aligned indexfonds.</p>
<h3>Voorbeelden van geformuleerde CO2-doelen</h3>
<ul>{"".join(f"<li><strong>{E(r['name'])}</strong>: {E(r.co2_doel)} {bron(int(r.fund_id),'esg')}</li>" for _, r in s[s.co2_doel.notna()].sort_values('aum_euro_bn', ascending=False).head(8).iterrows())}</ul>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.esg_instrumenten.notna(), 'esg')}</ul></div>""")

# 7 regio
P.append(f"""<h2 id="regio">7. Regioverdeling</h2>
<p>Een regioverdeling van de aandelen staat bij {reg.fund_id.nunique()} fondsen ({", ".join(f"{E(i)} {int(x)}" for i, x in reg.groupby('basis').fund_id.nunique().items())}). Alleen de {vol.fund_id.nunique()} fondsen met een volledige verdeling (som 90–110%) zijn gemiddeld.</p>
<div class="fig"><h4>Gemiddeld regiogewicht in de aandelenportefeuille</h4><p class="sub">Gemiddelde over de fondsen die de regio noemen; "Wereld ontwikkeld" bij fondsen die alleen ontwikkeld/opkomend onderscheiden</p><canvas id="c-regio" height="240"></canvas></div>
{tabel([[E(i), int(r.fondsen), f1(r.gem), f1(r.med)] for i, r in rg.iterrows()], ["regio", "fondsen", "gemiddeld %", "mediaan %"])}
<p>Het gewicht van opkomende markten ligt mediaan op {f0(em.median())}% (n={len(em)}). Een expliciete Europa-overweging komt voor bij een handvol ondernemingsfondsen, zoals Hoogovens met 40% Europa en 35% Verenigde Staten {bron(106,'regio')}; Nederland als aparte regio noemt {int((reg.regio=='Nederland').sum())} fonds.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.fund_id.isin(vol.fund_id), 'regio')}</ul></div>""")

# 8 valuta
P.append(f"""<h2 id="valuta">8. Valuta</h2>
<p>Afdekking van het valutarisico op aandelen wordt bij {len(v)} fondsen genoemd: mediaan {f0(v.valuta_afgedekt_pct.median())}%, {int((v.valuta_afgedekt_pct==0).sum())} dekken niets af, {int((v.valuta_afgedekt_pct>=100).sum())} dekken volledig af. De meeste fondsen dekken de dollar en andere grote valuta's voor een vast deel af (25%, 50% of 75%) en laten opkomende-marktenvaluta open; ABP zit op 25% voor zakelijke waarden {bron(9,'valuta')}, PFZW hanteert een dynamische dollarafdekking van 40–60% {bron(41,'valuta')}, en Hoogovens dekt de dollar bewust niet af omdat die in crises bescherming biedt {bron(106,'valuta')}.</p>
<div class="bronnen"><h5>Bronnen (voorbeelden)</h5><ul>{voorbeelden(s.valuta_afgedekt_pct.notna(), 'valuta')}</ul></div>""")

# 9 kwaliteit
P.append(f"""<h2 id="kwaliteit">9. Datakwaliteit en voorbehouden</h2>
{tabel([[E(kk), vv, n, pct(n - vv, n)] for kk, vv in ontbreekt.items()], ["gegeven", "ontbreekt bij", "van", "dekking"])}
<ul>
<li>Een jaarverslag noemt zelden alles. Welke beheerder welk mandaat voert staat vaker in de ABTN of op de website dan in het verslag; {int((s.aantal_beheerders==0).sum())} fondsen noemen geen enkele aandelenbeheerder bij naam.</li>
<li>De uitlezing is door een taalmodel gedaan. De steekproef van vijf fondsen klopte volledig, maar een systematische tweede lezing van alle {n} fondsen is niet uitgevoerd. Bij twijfel is het citaat in het bronregister de toets.</li>
<li>Kostenpercentages per categorie zijn niet overal op dezelfde noemer gedefinieerd (gemiddeld belegd vermogen in de categorie versus totaal vermogen). De verslagen die ze noemen gebruiken vrijwel altijd de eerste.</li>
<li>De oudere velden in de database (<span class="mono">funds.equity_allocation_pct</span>, <span class="mono">equity_strategy.management_style</span>) wijken bij ruim de helft van de fondsen af van het verslag en zijn niet overschreven; dit verslag gebruikt uitsluitend de nieuwe uitlezing.</li>
</ul>""")

# register
P.append(f"""<h2 id="register">Bronregister</h2>
<p>Per fonds het gelezen jaarverslag, en per onderwerp de PDF-pagina en het letterlijke citaat waarop de waarde rust. Gesorteerd op vermogen; fondsen staan onder de naam uit de database met tussen haakjes de eigen afkorting uit het verslag (PDN voor DSM Nederland, StiPP voor Personeelsdiensten). Zoeken kan op naam, afkorting, uitvoerder of beheerder.</p>
<div class="filter"><label for="zoek">Zoek fonds of beheerder </label><input id="zoek" type="search" placeholder="bijv. Northern Trust, PDN, DPS, 106"></div>
<div id="register-lijst">{"".join(reg_rows)}</div>
<p class="mono" style="margin-top:24px">Gegenereerd op 2026-10-07 uit pension_funds.db (equity_setup_2025) en docs/aandelencontrole_2025/verslagcijfers/*.json.</p>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const D = {json.dumps(chart, ensure_ascii=False)};
const css = k => getComputedStyle(document.documentElement).getPropertyValue(k).trim();
function maak() {{
  if (!window.Chart) return;
  const ink2 = css('--ink2'), line = css('--line'), C = ['--c1','--c2','--c3','--c4','--c5'].map(css);
  Chart.defaults.color = ink2; Chart.defaults.borderColor = line; Chart.defaults.font.family = css('--body');
  const grid = {{color: line}};
  new Chart(document.getElementById('c-stijl'), {{type:'bar', data:{{labels:D.stijl.labels, datasets:[
    {{label:'passief', data:D.stijl.series.passief, backgroundColor:C[0]}}, {{label:'gemengd', data:D.stijl.series.gemengd, backgroundColor:C[1]}},
    {{label:'actief', data:D.stijl.series.actief, backgroundColor:C[2]}}, {{label:'onbekend', data:D.stijl.series.onbekend, backgroundColor:C[4]}}]}},
    options:{{indexAxis:'y', responsive:true, scales:{{x:{{stacked:true, grid, ticks:{{precision:0}}, title:{{display:true,text:'fondsen'}}}}, y:{{stacked:true, grid:{{display:false}}}}}}, plugins:{{legend:{{position:'bottom'}}}}, borderSkipped:false, borderRadius:2, barPercentage:.6}}}});
  new Chart(document.getElementById('c-beheerders'), {{type:'bar', data:{{labels:D.beheerders.labels, datasets:[{{label:'fondsen', data:D.beheerders.fondsen, backgroundColor:C[0], borderRadius:2}}]}},
    options:{{indexAxis:'y', responsive:true, scales:{{x:{{grid, ticks:{{precision:0}}, title:{{display:true,text:'fondsen'}}}}, y:{{grid:{{display:false}}}}}}, plugins:{{legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`${{c.parsed.x}} fondsen · ${{D.beheerders.mandaten[c.dataIndex]}} mandaten`}}}}}}, barPercentage:.65}}}});
  const stijlen = Object.keys(D.kosten);
  new Chart(document.getElementById('c-kosten'), {{type:'scatter', data:{{datasets: stijlen.map((st,i)=>({{label:st, data:D.kosten[st].map((x,j)=>({{x, y:i+((j%3)-1)*0.12}})), backgroundColor:C[i], pointRadius:5, pointHoverRadius:7}}))}},
    options:{{responsive:true, scales:{{x:{{grid, title:{{display:true,text:'% van het belegd vermogen in aandelen'}}, ticks:{{callback:v=>v.toFixed(2).replace('.',',')+'%'}}}}, y:{{min:-0.6, max:stijlen.length-0.4, grid:{{display:false}}, ticks:{{stepSize:1, callback:v=>stijlen[v]??''}}}}}}, plugins:{{legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`${{c.dataset.label}}: ${{c.parsed.x.toFixed(2).replace('.',',')}}%`}}}}}}}}}});
  new Chart(document.getElementById('c-esg'), {{type:'bar', data:{{labels:D.esg.labels, datasets:[{{label:'fondsen', data:D.esg.waarden, backgroundColor:C[2], borderRadius:2}}]}},
    options:{{indexAxis:'y', responsive:true, scales:{{x:{{grid, ticks:{{precision:0}}, title:{{display:true,text:'fondsen'}}}}, y:{{grid:{{display:false}}}}}}, plugins:{{legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`${{c.parsed.x}} van ${{D.esg.n}} fondsen (${{Math.round(100*c.parsed.x/D.esg.n)}}%)`}}}}}}, barPercentage:.65}}}});
  new Chart(document.getElementById('c-regio'), {{type:'bar', data:{{labels:D.regio.labels, datasets:[{{label:'gemiddeld %', data:D.regio.gem, backgroundColor:C[0], borderRadius:2}}]}},
    options:{{indexAxis:'y', responsive:true, scales:{{x:{{grid, title:{{display:true,text:'gemiddeld gewicht, %'}}}}, y:{{grid:{{display:false}}}}}}, plugins:{{legend:{{display:false}}, tooltip:{{callbacks:{{label:c=>`${{c.parsed.x}}% gemiddeld over ${{D.regio.fondsen[c.dataIndex]}} fondsen`}}}}}}, barPercentage:.65}}}});
}}
maak();
const zoek = document.getElementById('zoek');
zoek.addEventListener('input', () => {{
  const q = zoek.value.trim().toLowerCase();
  document.querySelectorAll('#register-lijst details').forEach(d => {{ d.hidden = q !== '' && !d.textContent.toLowerCase().includes(q); }});
}});
if (location.hash.startsWith('#bron-')) {{ const d = document.querySelector(location.hash); if (d) d.open = true; }}
window.addEventListener('hashchange', () => {{ const d = document.querySelector(location.hash); if (d && d.tagName === 'DETAILS') d.open = true; }});
</script>
""")
open(UIT, "w").write("\n".join(P))
print("geschreven:", UIT, os.path.getsize(UIT) // 1024, "KB")
