"""Sectoranalyse van de inrichting van de aandelenstrategie, uit equity_setup_2025 en zustertabellen.

Schrijft docs/aandelencontrole_2025/AANDELENSTRATEGIE_2025.md en vier figuren in plots/aandelen_2025/.
Draaien met .venv/bin/python (pandas + matplotlib).
"""
import os, sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

S = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.expanduser("~/pensioenfondsen-app")
DB = os.path.join(BASE, "data/processed/pension_funds.db")
PLOTS = os.path.join(BASE, "plots/aandelen_2025")
UIT = os.path.join(S, "AANDELENSTRATEGIE_2025.md")
os.makedirs(PLOTS, exist_ok=True)
KLEUR = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2,
                     "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 150})

con = sqlite3.connect(DB)
s = pd.read_sql("""select e.*, f.name, f.aum_euro_bn, f.category from equity_setup_2025 e join funds f on f.id=e.fund_id""", con)
beh = pd.read_sql("select b.*, f.name as fonds, f.aum_euro_bn from equity_setup_beheerders_2025 b join funds f on f.id=b.fund_id", con)
reg = pd.read_sql("select r.*, f.aum_euro_bn from equity_setup_regio_2025 r join funds f on f.id=r.fund_id", con)
# zelfde populatie in teller en noemer: alle rijen met een vermogen (kringen en koepels tellen allebei mee)
totaal_fondsen, totaal_aum = con.execute("select count(*), sum(aum_euro_bn) from funds where aum_euro_bn is not null").fetchone()

COHORT = [(25, "> 25 mld"), (5, "5–25 mld"), (1, "1–5 mld"), (0, "< 1 mld")]
def cohort(a):
    if pd.isna(a): return "onbekend"
    for grens, naam in COHORT:
        if a >= grens: return naam
s["cohort"] = s["aum_euro_bn"].map(cohort)
volgorde = [n for _, n in COHORT]
# strategisch gewicht telt alleen mee als het op de totale portefeuille slaat; een normgewicht binnen de
# rendementsportefeuille (bv. 71% bij de Centraal Beheer-kringen) is geen allocatie van het hele vermogen
s["strategisch_totaal"] = s["strategisch_pct"].where(s["strategisch_basis"] == "totale portefeuille")
s["allocatie_beste"] = s["feitelijk_pct"].fillna(s["berekend_pct"]).fillna(s["strategisch_totaal"])
s["allocatie_bron"] = s.apply(lambda r: "feitelijk (verslag)" if pd.notna(r.feitelijk_pct) else "feitelijk (berekend)" if pd.notna(r.berekend_pct)
                              else "strategisch" if pd.notna(r.strategisch_totaal) else "geen", axis=1)

R = []
def h(t, n=2): R.append("\n" + "#" * n + " " + t + "\n")
def p(t): R.append(t + "\n")
def tabel(df, kolommen=None, idx=False, fmt="{:.1f}"):
    df = df if kolommen is None else df[kolommen]
    kop = list(df.columns) if not idx else [df.index.name or ""] + list(df.columns)
    R.append("| " + " | ".join(str(k) for k in kop) + " |")
    R.append("|" + "|".join(["---"] * len(kop)) + "|")
    for i, rij in df.iterrows():
        cellen = ([str(i)] if idx else []) + [("" if pd.isna(x) else str(int(x)) if (k in ("fondsen", "mandaten", "ontbreekt bij", "van") or float(x).is_integer() and fmt == "{:.0f}") else fmt.format(x)) if isinstance(x, (float, int)) and not isinstance(x, bool) else str(x) for k, x in zip(df.columns, rij)]
        R.append("| " + " | ".join(cellen) + " |")
    R.append("")
def pct(n, d): return f"{100 * n / d:.0f}%" if d else "–"
def eindig(ax, titel, xlab=""):
    ax.set_title(titel, loc="left", color=INK, fontsize=11, pad=10)
    ax.set_xlabel(xlab); ax.grid(axis="x", color=GRID, lw=0.6); ax.set_axisbelow(True)
    for sp in ("left",): ax.spines[sp].set_visible(False)
    ax.tick_params(length=0)
    if xlab == "fondsen": ax.xaxis.set_major_locator(MaxNLocator(integer=True))

n = len(s)
R.append("# De inrichting van de aandelenstrategie bij Nederlandse pensioenfondsen (jaarverslagen 2025)\n")
def mld(x): return f"{x:,.0f}".replace(",", ".")
p(f"Gelezen uit {n} jaarverslagen over 2025, samen € {mld(s.aum_euro_bn.sum())} miljard van de € {mld(totaal_aum)} miljard " +
  f"bij {totaal_fondsen} fondsen en kringen met een vermogen in de database ({pct(s.aum_euro_bn.sum(), totaal_aum)} van het vermogen). "
  f"Elk cijfer is door Gemini (gemini-3.8-flash-high) uit de verslagtekst gelezen, met pagina en letterlijke vindplaats in "
  f"`equity_setup_2025`; zie de datakwaliteitsparagraaf onderaan voor wat ontbreekt.")

h("1. Hoeveel aandelen")
g = s.groupby("cohort").agg(fondsen=("fund_id", "size"), feitelijk_mediaan=("allocatie_beste", "median"),
                            strategisch_mediaan=("strategisch_totaal", "median"), private_equity_mediaan=("private_equity_pct", "median")).reindex(volgorde)
g.index.name = "cohort (vermogen)"
tabel(g, idx=True)
p(f"Bron van het allocatiecijfer: " + ", ".join(f"{k} {v}" for k, v in s.allocatie_bron.value_counts().items()) + ". "
  f"Bij {int((s.strategisch_basis == 'rendementsportefeuille').sum())} fondsen noemt het verslag het aandelengewicht alleen binnen de "
  f"rendementsportefeuille; dat is hier niet als allocatie geteld.")
pe = s[s.private_equity_pct.notna()]
p(f"{len(pe)} fondsen noemen een aparte private-equitypositie (mediaan {pe.private_equity_pct.median():.1f}%); "
  f"bij {int(s.omvat_private_equity.fillna(0).sum())} fondsen zit private equity in de aandelenpost zelf.")

h("2. Wie beheert het")
org = pd.crosstab(s.cohort, s.organisatie).reindex(volgorde).fillna(0).astype(int)
org.index.name = "cohort"
tabel(org, idx=True, fmt="{:.0f}")
fid = s[s.fiduciair_norm.notna()].groupby("fiduciair_norm").agg(fondsen=("fund_id", "size"), vermogen_mld=("aum_euro_bn", "sum")).sort_values("fondsen", ascending=False)
fid.index.name = "fiduciair / integraal beheerder"
p(f"Fiduciair beheerder genoemd bij {len(s[s.fiduciair_norm.notna()])} van de {n} fondsen:")
tabel(fid.head(15), idx=True)

h("3. Welke partij en welk product")
bm = beh.groupby("naam_norm").agg(mandaten=("id", "size"), fondsen=("fund_id", "nunique"), vermogen_mld=("aum_euro_bn", lambda x: beh.loc[x.index].drop_duplicates("fund_id").aum_euro_bn.sum()))
bm = bm.sort_values("fondsen", ascending=False)
bm.index.name = "aandelenbeheerder"
p(f"{len(beh)} aandelenmandaten bij {beh.fund_id.nunique()} fondsen, {beh.naam_norm.nunique()} verschillende partijen. "
  f"Vermogen = het totale fondsvermogen van de fondsen die de partij noemen, niet de omvang van het mandaat.")
tabel(bm.head(20), idx=True)
stijl_per = pd.crosstab(beh.naam_norm, beh.stijl).loc[bm.head(12).index]
stijl_per.index.name = "beheerder"
p("Beheerstijl per mandaat bij de twaalf meest genoemde partijen:")
tabel(stijl_per, idx=True, fmt="{:.0f}")
prod = beh[beh.product_of_mandaat.notna()].copy()
p(f"Voorbeelden van genoemde producten/mandaten ({len(prod)} met productnaam):")
for naam_norm, grp in prod.groupby("naam_norm"):
    if naam_norm in bm.head(10).index:
        voorbeelden = sorted(set(grp.product_of_mandaat))[:6]
        p(f"- **{naam_norm}**: " + "; ".join(voorbeelden))

h("4. Passief of actief")
st = pd.crosstab(s.cohort, s.hoofdstijl).reindex(volgorde).fillna(0).astype(int)
st.index.name = "cohort"
tabel(st, idx=True, fmt="{:.0f}")
pp = s[s.passief_pct.notna()]
p(f"{len(pp)} fondsen noemen een percentage passief beheerd: {int((pp.passief_pct >= 99).sum())} daarvan volledig passief, "
  f"de overige tussen {pp[pp.passief_pct < 99].passief_pct.min():.0f}% en {pp[pp.passief_pct < 99].passief_pct.max():.0f}%. "
  f"Factorbeleggen expliciet bij {int((s.factorbeleggen == 1).sum())} fondsen, expliciet niet bij {int((s.factorbeleggen == 0).sum())}.")
bmk = s.benchmarks.dropna().str.split("; ").explode().str.strip()
fam = bmk.str.extract(r"(MSCI|FTSE|STOXX|S&P|Solactive|Bloomberg|Russell)", expand=False).fillna("overig/eigen")
p("Benchmarkfamilies voor aandelen: " + ", ".join(f"{k} {v}" for k, v in fam.value_counts().items()) + ". "
  f"Maatwerk/ESG-variant van de index (eigen_index) bij {int((s.eigen_index == 1).sum())} fondsen.")

h("5. Wat het kost")
k = s[s.beheerkosten_aandelen_pct.notna()]
p(f"Beheerkosten van de aandelencategorie bij {len(k)} fondsen genoemd: mediaan {k.beheerkosten_aandelen_pct.median():.2f}% "
  f"(kwartielen {k.beheerkosten_aandelen_pct.quantile(.25):.2f}–{k.beheerkosten_aandelen_pct.quantile(.75):.2f}%).")
ks = k.groupby("hoofdstijl").agg(fondsen=("fund_id", "size"), mediaan_pct=("beheerkosten_aandelen_pct", "median"),
                                 q25=("beheerkosten_aandelen_pct", lambda x: x.quantile(.25)), q75=("beheerkosten_aandelen_pct", lambda x: x.quantile(.75)))
ks.index.name = "hoofdstijl"
tabel(ks, idx=True, fmt="{:.2f}")
kc = k.groupby("cohort").agg(fondsen=("fund_id", "size"), mediaan_pct=("beheerkosten_aandelen_pct", "median")).reindex(volgorde)
kc.index.name = "cohort"
tabel(kc, idx=True, fmt="{:.2f}")
kt = s[s.vermogensbeheerkosten_totaal_pct.notna()]
p(f"Fondsbrede vermogensbeheerkosten (alle categorieën) bij {len(kt)} fondsen: mediaan {kt.vermogensbeheerkosten_totaal_pct.median():.2f}%; "
  f"transactiekosten aandelen bij {int(s.transactiekosten_aandelen_pct.notna().sum())} fondsen (mediaan "
  f"{s.transactiekosten_aandelen_pct.median():.2f}%); prestatievergoeding op aandelen bij {int(s.prestatievergoeding_aandelen_eur_mln.notna().sum())}.")

h("6. De rol van ESG")
instr = s.esg_instrumenten.dropna().str.split("; ").explode().str.strip().value_counts()
ins = pd.DataFrame({"fondsen": instr, "aandeel": [pct(v, n) for v in instr]})
ins.index.name = "instrument"
tabel(ins, idx=True, fmt="{:.0f}")
sf = s.sfdr_artikel.value_counts().sort_index()
p("SFDR-artikel: " + ", ".join(f"artikel {int(k)} {v}" for k, v in sf.items()) + f", niet genoemd {int(s.sfdr_artikel.isna().sum())}. "
  f"CO2-reductiedoel geformuleerd bij {int(s.co2_doel.notna().sum())} fondsen.")
p("Voorbeelden van CO2-doelen:")
for _, r in s[s.co2_doel.notna()].sort_values("aum_euro_bn", ascending=False).head(8).iterrows():
    p(f"- {r['name']}: {r.co2_doel}")

h("7. Regioverdeling")
rb = reg.basis.value_counts()
p(f"Regioverdeling genoemd bij {reg.fund_id.nunique()} fondsen (" + ", ".join(f"{k} {v}" for k, v in reg.groupby('basis').fund_id.nunique().items()) + ").")
# alleen fondsen met een volledige verdeling (som 90-110) middelen
som = reg.groupby("fund_id").pct.sum()
vol = reg[reg.fund_id.isin(som[(som > 90) & (som < 110)].index)]
rg = vol.groupby("regio").agg(fondsen=("fund_id", "nunique"), gemiddeld_pct=("pct", "mean"), mediaan_pct=("pct", "median")).sort_values("gemiddeld_pct", ascending=False)
rg.index.name = "regio"
p(f"Bij de {vol.fund_id.nunique()} fondsen met een volledige verdeling (som 90–110%):")
tabel(rg, idx=True)
em = vol[vol.regio == "Opkomende markten"].pct
p(f"Gewicht opkomende markten: mediaan {em.median():.0f}% (n={len(em)}); Nederland apart genoemd bij {int((reg.regio == 'Nederland').sum())} fondsen.")

h("8. Valuta")
v = s[s.valuta_afgedekt_pct.notna()]
p(f"Afdekking van het valutarisico op aandelen genoemd bij {len(v)} fondsen: mediaan {v.valuta_afgedekt_pct.median():.0f}%, "
  f"{int((v.valuta_afgedekt_pct == 0).sum())} dekken niets af, {int((v.valuta_afgedekt_pct >= 100).sum())} dekken volledig af.")

h("9. Wat ontbreekt en wat afwijkt")
leeg = {"aandelenallocatie": s.allocatie_beste.isna().sum(), "beheerder genoemd": (s.aantal_beheerders == 0).sum(),
        "hoofdstijl onbekend": (s.hoofdstijl == "onbekend").sum(), "beheerkosten aandelen": s.beheerkosten_aandelen_pct.isna().sum(),
        "SFDR-artikel": s.sfdr_artikel.isna().sum(), "regioverdeling": (~s.fund_id.isin(reg.fund_id)).sum()}
tabel(pd.DataFrame({"veld": list(leeg), "ontbreekt bij": [int(x) for x in leeg.values()], "van": [n] * len(leeg)}), fmt="{:.0f}")
p("Vergelijking met de oudere velden in `funds`/`equity_strategy` staat in de uitvoer van `laad_aandelen.py`.")

# ---- figuren --------------------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(7, 3.2))
stn = st.reindex(columns=[c for c in ("passief", "gemengd", "actief", "onbekend") if c in st.columns]).fillna(0)
links = pd.Series(0, index=stn.index)
for i, c in enumerate(stn.columns):
    ax.barh(stn.index, stn[c], left=links, color=KLEUR[i] if c != "onbekend" else GRID, height=0.55, label=c, edgecolor="white", linewidth=1.5)
    for y, (l, w) in enumerate(zip(links, stn[c])):
        if w >= 3: ax.text(l + w / 2, y, f"{int(w)}", ha="center", va="center", color="white" if c != "onbekend" else INK2, fontsize=8)
    links = links + stn[c]
ax.invert_yaxis(); ax.legend(frameon=False, ncol=4, loc="lower center", bbox_to_anchor=(0.5, -0.35))
eindig(ax, "Hoofdstijl van de aandelenportefeuille per vermogenscohort (aantal fondsen)", "fondsen")
fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "stijl_per_cohort.png")); plt.close(fig)

fig, ax = plt.subplots(figsize=(7, 3.2))
orde = [c for c in ("passief", "gemengd", "actief") if c in ks.index]
for i, c in enumerate(orde):
    y = k[k.hoofdstijl == c].beheerkosten_aandelen_pct
    ax.scatter(y, [i] * len(y) + (pd.Series(range(len(y))) % 3 - 1) * 0.08, s=22, color=KLEUR[i], alpha=0.75, edgecolor="white", linewidth=0.8)
    ax.plot([y.median()] * 2, [i - 0.3, i + 0.3], color=INK, lw=2)
    ax.text(y.median() + 0.012, i - 0.3, f"mediaan {y.median():.2f}%", ha="left", va="bottom", fontsize=8, color=INK2)
ax.set_yticks(range(len(orde))); ax.set_yticklabels(orde); ax.invert_yaxis()
eindig(ax, "Beheerkosten van de aandelencategorie, per hoofdstijl", "% van het belegd vermogen in aandelen")
fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "kosten_per_stijl.png")); plt.close(fig)

fig, ax = plt.subplots(figsize=(7, 4.2))
top = bm.head(15).sort_values("fondsen")
ax.barh(top.index, top.fondsen, color=KLEUR[0], height=0.6)
for y, v_ in enumerate(top.fondsen): ax.text(v_ + 0.3, y, str(int(v_)), va="center", fontsize=8, color=INK2)
eindig(ax, "Meest genoemde aandelenbeheerders (aantal fondsen)", "fondsen")
fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "beheerders_top.png")); plt.close(fig)

fig, ax = plt.subplots(figsize=(7, 3.4))
ins2 = instr.sort_values()
ax.barh(ins2.index, ins2.values, color=KLEUR[2], height=0.6)
for y, v_ in enumerate(ins2.values): ax.text(v_ + 0.3, y, f"{int(v_)} ({pct(v_, n)})", va="center", fontsize=8, color=INK2)
eindig(ax, "ESG-instrumenten op de aandelenportefeuille (aantal fondsen)", "fondsen")
fig.tight_layout(); fig.savefig(os.path.join(PLOTS, "esg_instrumenten.png")); plt.close(fig)

passief_aandeel = pct(int((s.hoofdstijl == "passief").sum()), n)
gemengd_aandeel = pct(int((s.hoofdstijl == "gemengd").sum()), n)
actief_aandeel = pct(int((s.hoofdstijl == "actief").sum()), n)
nt_blk = bm.loc[[x for x in ("Northern Trust", "BlackRock", "Aegon AM") if x in bm.index], "fondsen"].sum()
kp, ka = k[k.hoofdstijl == "passief"].beheerkosten_aandelen_pct.median(), k[k.hoofdstijl == "actief"].beheerkosten_aandelen_pct.median()
SAMENVATTING = f"""
## Samenvatting

- **Passief is de norm, actief de uitzondering.** {passief_aandeel} van de fondsen belegt de aandelen (vrijwel) volledig passief, {gemengd_aandeel} combineert een passieve kern met actieve satellieten (meestal opkomende markten of small caps), {actief_aandeel} is overwegend actief. Hoe kleiner het fonds, hoe vaker volledig passief; de grote fondsen zijn vaker gemengd omdat ze eigen maatwerkindices en actieve mandaten voeren.
- **Drie partijen bedienen de helft van de markt.** Northern Trust, BlackRock en Aegon AM worden samen door {int(nt_blk)} van de {beh.fund_id.nunique()} fondsen met een genoemde beheerder gebruikt, vrijwel altijd via gepoolde indexfondsen (CCF/FGR-structuren) met een ESG-screening of Paris-aligned variant. Robeco is de grootste actieve/enhanced partij (QI Conservative en Enhanced Index). De fiduciaire laag is nog geconcentreerder: Achmea IM, Aegon AM, Goldman Sachs AM, Van Lanschot Kempen en Columbia Threadneedle bedienen samen {int(fid.head(5).fondsen.sum())} van de {int(fid.fondsen.sum())} fondsen met een genoemde fiduciair.
- **Het product is bijna altijd een MSCI-index met een ESG-filter.** MSCI komt {int((fam == "MSCI").sum())} keer voor als benchmark, FTSE {int((fam == "FTSE").sum())} keer; {int((s.eigen_index == 1).sum())} fondsen (vooral de grote) laten een eigen maatwerkindex bouwen. Northern Trust World SDG Screened Low Carbon, BlackRock Developed World ESG Screened en de Paris-Aligned-varianten zijn de standaardbouwstenen.
- **Passief kost een fractie van actief.** Waar het verslag de kosten per categorie noemt ({len(k)} fondsen) betaalt een passief fonds mediaan {kp:.2f}% van het belegd vermogen in aandelen, een actief fonds {ka:.2f}%; de grootste fondsen betalen het meest omdat zij actieve en illiquide strategieën voeren.
- **ESG is standaarduitrusting, klimaatdoelen niet.** Uitsluitingen ({pct(int(instr.get("uitsluitingen", 0)), n)}), engagement en stembeleid zijn vrijwel universeel; een geformuleerd CO2-reductiedoel heeft {pct(int(s.co2_doel.notna().sum()), n)} van de fondsen, een Paris-aligned benchmark {pct(int(instr.get("Paris-aligned benchmark", 0)), n)}. {int((s.sfdr_artikel == 8).sum())} fondsen zijn SFDR artikel 8, {int((s.sfdr_artikel == 6).sum())} artikel 6; artikel 9 komt niet voor.
- **Regionaal volgt men de wereldindex.** Waar een volledige verdeling wordt genoemd ligt Noord-Amerika mediaan rond {rg.loc["Noord-Amerika", "mediaan_pct"]:.0f}% en opkomende markten rond {em.median():.0f}%; een expliciete afwijking van de marktkapitalisatie (Europa-overweging, Nederland) is zeldzaam.
- **Valutarisico op aandelen wordt meestal half afgedekt.** Mediaan {v.valuta_afgedekt_pct.median():.0f}% bij {len(v)} fondsen; volledig afdekken en helemaal niet afdekken komen allebei voor, het laatste met het argument dat de dollar in crises bescherming biedt.
- **De aandelenallocatie zelf** ligt mediaan rond {s.allocatie_beste.median():.0f}% van het vermogen en is bij kleine fondsen hoger dan bij grote, waar private equity en andere illiquide categorieën een deel van het risicobudget innemen.
"""
R.insert(R.index("\n## 1. Hoeveel aandelen\n"), SAMENVATTING)
R.insert(R.index("\n## 3. Welke partij en welk product\n"), "![beheerders](../../plots/aandelen_2025/beheerders_top.png)\n")
R.insert(R.index("\n## 5. Wat het kost\n"), "![stijl](../../plots/aandelen_2025/stijl_per_cohort.png)\n")
R.insert(R.index("\n## 6. De rol van ESG\n"), "![kosten](../../plots/aandelen_2025/kosten_per_stijl.png)\n")
R.insert(R.index("\n## 7. Regioverdeling\n"), "![esg](../../plots/aandelen_2025/esg_instrumenten.png)\n")
open(UIT, "w").write("\n".join(R))
print("geschreven:", UIT, "en 4 figuren in", PLOTS)
