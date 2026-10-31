import time
import pandas as pd
import altair as alt
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Compteur intelligent", layout="wide")
db = MongoClient("mongodb://localhost:27017")["power"]

JOURS = {1: "Dim", 2: "Lun", 3: "Mar", 4: "Mer", 5: "Jeu", 6: "Ven", 7: "Sam"}

# --- Barre laterale ---
st.sidebar.header("Reglages")
nb = st.sidebar.slider("Fenetres affichees (15 min chacune)", 50, 1000, 300)
seuil = st.sidebar.slider("Seuil d'alerte (z-score)", 0.5, 5.0, 2.0, 0.1)
auto = st.sidebar.checkbox("Rafraichir automatiquement (3 s)", True)

st.title("Surveillance de la consommation electrique d'un foyer")

fenetres = list(db["windows"].find({}, {"_id": 0}).sort("start", -1).limit(nb))
profils = list(db["profiles"].find({}, {"_id": 0}))

if not fenetres:
    st.info("Aucune donnee : lance le streaming et le producteur.")
else:
    df = pd.DataFrame(fenetres).sort_values("start")
    df["z"] = df["z"].fillna(0)
    df["alerte"] = df["z"] > seuil
    der = df.iloc[-1]

    # --- Banniere d'etat ---
    if der["alerte"]:
        st.error(f"ALERTE : {der['avg_kw']:.2f} kW mesures, normale {der['mean_kw']:.2f} kW (z = {der['z']:.1f})")
    else:
        st.success(f"Consommation normale : {der['avg_kw']:.2f} kW (z = {der['z']:.1f})")

    # --- Chiffres cles ---
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Derniere fenetre", str(der["start"])[:16])
    k2.metric("Mesuree (kW)", f"{der['avg_kw']:.2f}", f"{der['avg_kw'] - der['mean_kw']:+.2f} vs normale")
    k3.metric("Fenetres analysees", db["windows"].count_documents({}))
    k4.metric(f"Alertes (z > {seuil})", int(df["alerte"].sum()))

    # --- Courbe mesuree vs normale, avec alertes en rouge ---
    st.subheader("Consommation mesuree et consommation normale")
    long = df.melt("start", ["avg_kw", "mean_kw"], "serie", "kw")
    long["serie"] = long["serie"].map({"avg_kw": "Mesuree", "mean_kw": "Normale"})
    ligne = alt.Chart(long).mark_line().encode(
        x=alt.X("start:T", title="Temps"), y=alt.Y("kw:Q", title="kW"),
        color=alt.Color("serie:N", title=""))
    points = alt.Chart(df[df["alerte"]]).mark_circle(size=90, color="red").encode(
        x="start:T", y="avg_kw:Q", tooltip=["start", "avg_kw", "mean_kw", "z"])
    st.altair_chart((ligne + points).interactive(), use_container_width=True)

    # --- Courbe du z-score ---
    st.subheader("Z-score (ecart a la normale)")
    zc = alt.Chart(df).mark_area(opacity=0.4).encode(x="start:T", y=alt.Y("z:Q", title="z"))
    regle = alt.Chart(pd.DataFrame({"y": [seuil]})).mark_rule(
        color="red", strokeDash=[5, 5]).encode(y="y:Q")
    st.altair_chart(zc + regle, use_container_width=True)

    # --- Alertes + carte thermique ---
    g, d = st.columns(2)
    with g:
        st.subheader("Dernieres alertes")
        al = df[df["alerte"]].sort_values("start", ascending=False).head(15)
        if al.empty:
            st.write("Aucune alerte.")
        else:
            st.dataframe(al[["start", "avg_kw", "mean_kw", "z"]], use_container_width=True)
    with d:
        st.subheader("Profil normal appris par le batch (kW)")
        if profils:
            hp = pd.DataFrame(profils)
            hp["jour"] = hp["dow"].map(JOURS)
            carte = alt.Chart(hp).mark_rect().encode(
                x=alt.X("hour:O", title="Heure"),
                y=alt.Y("jour:N", sort=list(JOURS.values()), title=""),
                color=alt.Color("mean_kw:Q", title="kW"),
                tooltip=["jour", "hour", "mean_kw", "std_kw"])
            st.altair_chart(carte, use_container_width=True)

if auto:
    time.sleep(3)
    st.rerun()