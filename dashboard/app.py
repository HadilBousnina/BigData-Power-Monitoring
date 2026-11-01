"""Dashboard Streamlit : surveillance de la consommation electrique d'un foyer.

Lit MongoDB (base "power") alimentee par Spark :
  profiles (batch), windows (streaming), daily_stats (batch optionnel).
4 onglets : temps reel, profil de reference, historique batch, etat du pipeline.
Lancer : python -m streamlit run dashboard\\app.py
"""
import os
import time
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB = os.getenv("MONGO_DB", "power")

# Spark dayofweek : 1 = dimanche ... 7 = samedi
JOURS = {1: "Dimanche", 2: "Lundi", 3: "Mardi", 4: "Mercredi", 5: "Jeudi", 6: "Vendredi", 7: "Samedi"}
ORDRE_JOURS = [2, 3, 4, 5, 6, 7, 1]
ROUGE, BLEU, GRIS = "#d62728", "#1f77b4", "#7f7f7f"

st.set_page_config(page_title="Smart Meter Monitor", page_icon="⚡", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[DB]


db = get_db()

# ------------------------------------------------------------ barre laterale
with st.sidebar:
    st.header("Parametres")
    n_hours = st.slider("Heures affichees", 12, 24 * 14, 72, step=12)
    Z = st.slider("Seuil d'alerte (z-score)", 0.5, 5.0, 2.0, 0.1)
    auto = st.checkbox("Rafraichissement automatique", value=True)
    every = st.slider("Intervalle (secondes)", 1, 10, 3)
    st.caption(f"Alerte si z >= {Z:g} ; critique si z >= {1.5 * Z:g}")
    st.divider()
    st.caption("Kafka -> Spark Structured Streaming -> MongoDB. Chaque point est la "
               "puissance moyenne d'une fenetre de 15 minutes, comparee au comportement "
               "normal calcule en batch sur HDFS.")

st.title("⚡ Surveillance de la consommation electrique")

try:
    db.command("ping")
except Exception as e:
    st.error(f"MongoDB injoignable : {e}")
    st.stop()


def spark_dow(ts):
    """Jour de semaine au format Spark (1 = dimanche) a partir d'un Timestamp pandas."""
    return (ts.dayofweek + 1) % 7 + 1


def load_windows(limit):
    rows = list(db.windows.find({}, {"_id": 0}).sort("start", -1).limit(limit))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("start").reset_index(drop=True)
    prof = {(p["dow"], p["hour"]): p["std_kw"] for p in db.profiles.find({}, {"_id": 0})}
    df["std_kw"] = [prof.get((spark_dow(t), t.hour)) for t in df["start"]]
    df["z"] = df["z"].astype(float)
    df["alert"] = df["z"] >= Z
    return df


def severity(z):
    if pd.isna(z):
        return "n/a"
    return "Critique" if z >= 1.5 * Z else "Alerte" if z >= Z else "Normal"


def flux_status():
    """Age de la derniere ecriture du job streaming (collection status)."""
    doc = db.status.find_one({"_id": "streaming"})
    if not doc:
        return "⚪ Aucune ecriture", None
    age = (datetime.utcnow() - doc["last_write"]).total_seconds()
    etat = "🟢 Flux actif" if age <= 15 else "🟠 Ralenti" if age <= 120 else "🔴 Arrete"
    return etat, age


def fmt_age(age):
    if age is None:
        return "n/a"
    return f"il y a {int(age)} s" if age < 120 else f"il y a {int(age // 60)} min"


# ------------------------------------------------------------ onglet 1 : temps reel
def realtime_view():
    df = load_windows(n_hours * 4)
    if df.empty:
        st.info("Aucune donnee en streaming pour l'instant. Lancez le job Spark Streaming, "
                "puis le producteur.")
        return

    total = db.windows.count_documents({})
    n_alerts = db.windows.count_documents({"z": {"$gte": Z}})
    last = df.iloc[-1]
    etat, age = flux_status()

    f1, f2_, f3 = st.columns(3)
    f1.metric("Etat du flux", etat)
    f2_.metric("Derniere evolution (Spark -> MongoDB)", fmt_age(age))
    f3.metric("Derniere fenetre", f"{last['start']:%Y-%m-%d %H:%M}")

    delta = None if pd.isna(last.get("mean_kw")) else last["avg_kw"] - last["mean_kw"]
    statut = severity(last["z"])
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Fenetres analysees", f"{total:,}".replace(",", " "))
    c2.metric("Alertes detectees", n_alerts, f"{100 * n_alerts / max(total, 1):.1f} % des fenetres",
              delta_color="off")
    c3.metric("Derniere fenetre (kW)", f'{last["avg_kw"]:.2f}',
              None if delta is None else f"{delta:+.2f} vs reference", delta_color="inverse")
    c4.metric("z-score", "n/a" if pd.isna(last["z"]) else f'{last["z"]:+.1f}')
    c5.metric("Statut", {"Normal": "🟢 Normal", "Alerte": "🟠 Alerte",
                         "Critique": "🔴 Critique"}.get(statut, "n/a"))

    # Courbe : consommation, bande normale (reference +/- Z ecarts-types) et alertes
    haut = df["mean_kw"] + Z * df["std_kw"]
    bas = (df["mean_kw"] - Z * df["std_kw"]).clip(lower=0)
    al = df[df["alert"]]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["start"], y=haut, line=dict(width=0), hoverinfo="skip",
                             showlegend=False))
    fig.add_trace(go.Scatter(x=df["start"], y=bas, fill="tonexty", line=dict(width=0),
                             fillcolor="rgba(44,160,44,0.15)", name=f"Zone normale (±{Z:g} σ)",
                             hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=df["start"], y=df["mean_kw"], name="Reference batch",
                             line=dict(color=GRIS, dash="dot")))
    fig.add_trace(go.Scatter(x=df["start"], y=df["avg_kw"], name="Consommation",
                             line=dict(color=BLEU, width=2)))
    fig.add_trace(go.Scatter(x=al["start"], y=al["avg_kw"], mode="markers", name="Alerte",
                             marker=dict(color=ROUGE, size=10, symbol="x"),
                             customdata=al[["z"]],
                             hovertemplate="%{x}<br>%{y:.2f} kW<br>z = %{customdata[0]:.1f}<extra></extra>"))
    fig.update_layout(template="plotly_white", height=420, margin=dict(l=10, r=10, t=30, b=10),
                      yaxis_title="Puissance moyenne (kW)", hovermode="x unified",
                      legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Dernieres alertes")
        a = df[df["alert"]].sort_values("start", ascending=False).head(12).copy()
        if a.empty:
            st.success("Aucune alerte sur la periode affichee.")
        else:
            a["Gravite"] = a["z"].map(severity)
            a = a[["start", "avg_kw", "mean_kw", "z", "Gravite"]]
            a.columns = ["Debut fenetre", "Consommation (kW)", "Reference (kW)", "z-score", "Gravite"]
            st.dataframe(a.round(2), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Alertes par jour")
        pipeline = [{"$match": {"z": {"$gte": Z}}},
                    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$start"}},
                                "n": {"$sum": 1}}},
                    {"$sort": {"_id": 1}}]
        par_jour = list(db.windows.aggregate(pipeline))
        if par_jour:
            d = pd.DataFrame(par_jour).rename(columns={"_id": "Jour", "n": "Alertes"})
            f2 = go.Figure(go.Bar(x=d["Jour"], y=d["Alertes"], marker_color=ROUGE))
            f2.update_layout(template="plotly_white", height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(f2, use_container_width=True)
        else:
            st.caption("Pas encore d'alerte a afficher.")
    st.caption(f"Derniere mise a jour : {datetime.now():%H:%M:%S}")


# ------------------------------------------------------------ onglet 2 : profil de reference
def reference_view():
    ref = list(db.profiles.find({}, {"_id": 0}))
    if not ref:
        st.info("Profils absents : lancez profiles_power.py puis profiles_to_mongo.py.")
        return
    r = pd.DataFrame(ref)
    st.subheader("Consommation « normale » par jour de semaine et par heure")
    st.caption("Calcule par Spark (batch) sur 2006-2008. C'est la reference a laquelle "
               "le streaming compare chaque fenetre en direct.")
    mean = r.pivot(index="dow", columns="hour", values="mean_kw").reindex(ORDRE_JOURS)
    std = r.pivot(index="dow", columns="hour", values="std_kw").reindex(ORDRE_JOURS)
    fig = go.Figure(go.Heatmap(z=mean.values, x=[f"{h} h" for h in mean.columns],
                               y=[JOURS[d] for d in mean.index], colorscale="YlOrRd",
                               colorbar=dict(title="kW"),
                               hovertemplate="%{y} %{x}<br>moyenne : %{z:.2f} kW<extra></extra>"))
    fig.update_layout(template="plotly_white", height=380, margin=dict(l=10, r=10, t=10, b=10),
                      yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    jour = st.selectbox("Detail d'un jour", [JOURS[d] for d in ORDRE_JOURS])
    d = [k for k, v in JOURS.items() if v == jour][0]
    m, s = mean.loc[d], std.loc[d]
    f2 = go.Figure()
    f2.add_trace(go.Scatter(x=list(m.index), y=m + Z * s, line=dict(width=0), hoverinfo="skip",
                            showlegend=False))
    f2.add_trace(go.Scatter(x=list(m.index), y=(m - Z * s).clip(lower=0), fill="tonexty",
                            line=dict(width=0), fillcolor="rgba(44,160,44,0.15)",
                            name=f"±{Z:g} σ", hoverinfo="skip"))
    f2.add_trace(go.Scatter(x=list(m.index), y=m, name="Moyenne", line=dict(color=BLEU, width=2)))
    f2.update_layout(template="plotly_white", height=300, margin=dict(l=10, r=10, t=10, b=10),
                     xaxis_title="Heure de la journee", yaxis_title="kW")
    st.plotly_chart(f2, use_container_width=True)


# ------------------------------------------------------------ onglet 3 : historique batch
def history_view():
    daily = list(db.daily_stats.find({}, {"_id": 0}).sort("day", 1))
    if not daily:
        st.info("Statistiques journalieres absentes : lancez batch_daily.py (Spark batch).")
        return
    d = pd.DataFrame(daily)
    d["day"] = pd.to_datetime(d["day"])
    d = d.set_index("day")
    c1, c2, c3 = st.columns(3)
    c1.metric("Jours analyses", f"{len(d):,}".replace(",", " "))
    c2.metric("Consommation moyenne", f'{d["kwh"].mean():.1f} kWh/jour')
    c3.metric("Pic de puissance", f'{d["max_kw"].max():.1f} kW')

    st.subheader("Consommation journaliere")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d.index, y=d["kwh"], name="kWh/jour",
                             line=dict(color="rgba(31,119,180,0.35)", width=1)))
    fig.add_trace(go.Scatter(x=d.index, y=d["kwh"].rolling(30, min_periods=7).mean(),
                             name="Moyenne mobile 30 j", line=dict(color=BLEU, width=3)))
    fig.update_layout(template="plotly_white", height=340, margin=dict(l=10, r=10, t=10, b=10),
                      yaxis_title="kWh", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Repartition mensuelle par usage")
    cols = ["kwh_kitchen", "kwh_laundry", "kwh_heating_ac"]
    m = d[["kwh"] + cols].groupby(d.index.strftime("%Y-%m")).sum()
    m["Autres usages"] = (m["kwh"] - m[cols].sum(axis=1)).clip(lower=0)
    noms = {"kwh_kitchen": "Cuisine", "kwh_laundry": "Buanderie",
            "kwh_heating_ac": "Chauffe-eau / clim", "Autres usages": "Autres usages"}
    f2 = go.Figure([go.Bar(x=m.index, y=m[c], name=n) for c, n in noms.items()])
    f2.update_layout(barmode="stack", template="plotly_white", height=340,
                     margin=dict(l=10, r=10, t=10, b=10), yaxis_title="kWh",
                     legend=dict(orientation="h", y=1.1))
    st.plotly_chart(f2, use_container_width=True)


# ------------------------------------------------------------ onglet 4 : pipeline
def pipeline_view():
    st.subheader("Etat du pipeline")
    n_ref = db.profiles.count_documents({})
    n_day = db.daily_stats.count_documents({})
    n_w = db.windows.count_documents({})
    n_al = db.windows.count_documents({"z": {"$gte": Z}})
    last = db.windows.find_one({}, {"start": 1}, sort=[("start", -1)])
    rows = [
        ("Batch : profils de reference", n_ref, "168 attendus (7 jours x 24 h)", n_ref == 168),
        ("Batch : statistiques journalieres", n_day, "au moins une ligne (optionnel)", n_day > 0),
        ("Streaming : fenetres de 15 min", n_w, "croit pendant le rejeu", n_w > 0),
        (f"Streaming : alertes (z >= {Z:g})", n_al, "depend des anomalies", True),
    ]
    t = pd.DataFrame([(("✅" if ok else "⚠️"), nom, n, att) for nom, n, att, ok in rows],
                     columns=["", "Composant", "Documents MongoDB", "Attendu"])
    st.dataframe(t, use_container_width=True, hide_index=True)
    if last:
        st.caption(f"Derniere fenetre traitee (temps d'evenement) : {last['start']:%Y-%m-%d %H:%M}")
    st.markdown(
        "**Chaine de traitement** : fichier brut (HDFS) -> Spark batch (nettoyage, profils) -> "
        "MongoDB ; producteur -> Kafka -> Spark Structured Streaming (fenetres de 15 minutes, "
        "z-score) -> MongoDB -> ce dashboard.")


tab1, tab2, tab3, tab4 = st.tabs(["Temps reel", "Profil de reference", "Historique batch", "Pipeline"])
with tab1:
    # Seul cet onglet se rafraichit automatiquement (pas de rechargement de toute la page)
    st.fragment(run_every=every if auto else None)(realtime_view)()
with tab2:
    reference_view()
with tab3:
    history_view()
with tab4:
    pipeline_view()