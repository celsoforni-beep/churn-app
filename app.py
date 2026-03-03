import streamlit as st
import pandas as pd
import sqlite3
import hashlib
import os
from datetime import date

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Base Clientes - Novo / Retorno / Churn",
    layout="wide"
)

DB_PATH = "clientes.db"

# SALT (definido no Streamlit Secrets)
SALT = os.getenv("HASH_SALT", "ALTERAR_NO_STREAMLIT")

# =====================================================
# SEGURANÇA (HASH)
# =====================================================

def clean_document(doc: str) -> str:
    return "".join([c for c in str(doc) if c.isdigit()])


def normalize_email(email: str) -> str:
    return str(email).strip().lower()


def sha256_hash(value: str) -> str:
    raw = (SALT + value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# =====================================================
# DATABASE
# =====================================================

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pedidos (
            customer_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            data_pedido TEXT NOT NULL,
            mes_compra TEXT NOT NULL,
            valor_pedido REAL
        )
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_order
        ON pedidos(order_id)
    """)

    conn.commit()
    conn.close()

# =====================================================
# NORMALIZAÇÃO CSV
# =====================================================

def normalize_input(df: pd.DataFrame):

    cols = {c.strip().lower(): c for c in df.columns}

    required = ["order2", "creation d", "client document", "email"]

    for r in required:
        if r not in cols:
            raise ValueError(f"Coluna obrigatória ausente: {r}")

    base = df[[
        cols["client document"],
        cols["email"],
        cols["order2"],
        cols["creation d"]
    ]].copy()

    base.columns = [
        "client_document",
        "email",
        "order_id",
        "data_pedido"
    ]

    # DATA
    base["data_pedido"] = pd.to_datetime(
        base["data_pedido"],
        dayfirst=True,
        errors="coerce"
    ).dt.date

    base = base.dropna(subset=["data_pedido", "order_id"])

    # LIMPEZA
    doc_clean = base["client_document"].apply(clean_document)
    email_clean = base["email"].apply(normalize_email)

    # PRIORIDADE DOCUMENTO -> EMAIL
    identificador = doc_clean.copy()
    mask = identificador.str.len() == 0
    identificador.loc[mask] = email_clean.loc[mask]

    # HASH FINAL (SEM PII)
    base["customer_id"] = identificador.apply(
        lambda x: sha256_hash(str(x))
    )

    # MÊS
    base["data_pedido"] = base["data_pedido"].astype(str)
    base["mes_compra"] = base["data_pedido"].str[:7]

    # VALOR
    if "total value" in cols:
        valor = df[cols["total value"]].astype(str)

        valor = (
            valor
            .str.replace("R$", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )

        base["valor_pedido"] = pd.to_numeric(
            valor,
            errors="coerce"
        )
    else:
        base["valor_pedido"] = None

    # 🔐 RETORNA SEM CPF/EMAIL
    return base[
        [
            "customer_id",
            "order_id",
            "data_pedido",
            "mes_compra",
            "valor_pedido"
        ]
    ]

# =====================================================
# INSERT HISTÓRICO
# =====================================================

def upsert_pedidos(df):

    conn = get_conn()

    before = pd.read_sql(
        "SELECT COUNT(*) n FROM pedidos",
        conn
    )["n"][0]

    df.to_sql(
        "pedidos",
        conn,
        if_exists="append",
        index=False
    )

    conn.execute("""
        DELETE FROM pedidos
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM pedidos
            GROUP BY order_id
        )
    """)

    conn.commit()

    after = pd.read_sql(
        "SELECT COUNT(*) n FROM pedidos",
        conn
    )["n"][0]

    conn.close()

    return after - before

# =====================================================
# BASE CLIENTE
# =====================================================

def build_cliente_base(ref_date,
                       ativo_dias=90,
                       churn_dias=180):

    conn = get_conn()

    pedidos = pd.read_sql(
        "SELECT * FROM pedidos",
        conn
    )

    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(
        pedidos["data_pedido"]
    )

    g = pedidos.groupby("customer_id")

    base = pd.DataFrame({
        "customer_id": g["customer_id"].first(),
        "primeira_compra": g["data_pedido"].min(),
        "ultima_compra": g["data_pedido"].max(),
        "qtd_pedidos": g["order_id"].nunique(),
        "receita_total": g["valor_pedido"].sum()
    }).reset_index(drop=True)

    base["dias_sem_compra"] = (
        pd.to_datetime(ref_date)
        - base["ultima_compra"]
    ).dt.days

    base["status"] = "Ativo"
    base.loc[
        base["dias_sem_compra"] > ativo_dias,
        "status"
    ] = "Em risco"

    base.loc[
        base["dias_sem_compra"] > churn_dias,
        "status"
    ] = "Churn"

    return base.sort_values(
        "ultima_compra",
        ascending=False
    )

# =====================================================
# REPORT MENSAL
# =====================================================

def month_report(mes):

    conn = get_conn()
    pedidos = pd.read_sql(
        "SELECT * FROM pedidos",
        conn
    )
    conn.close()

    if pedidos.empty:
        return pd.DataFrame()

    pedidos["data_pedido"] = pd.to_datetime(
        pedidos["data_pedido"]
    )

    first = pedidos.groupby(
        "customer_id"
    )["data_pedido"].min().reset_index()

    first["mes_primeira"] = \
        first["data_pedido"].astype(str).str[:7]

    mes_df = pedidos[
        pedidos["mes_compra"] == mes
    ]

    clientes_mes = mes_df["customer_id"].nunique()

    novos = len(
        set(first[first["mes_primeira"] == mes]
            ["customer_id"])
        &
        set(mes_df["customer_id"])
    )

    retornantes = clientes_mes - novos

    receita = mes_df["valor_pedido"].sum()

    return pd.DataFrame([{
        "Mes": mes,
        "Clientes": clientes_mes,
        "Novos": novos,
        "Retornantes": retornantes,
        "Receita": receita
    }])

# =====================================================
# UI STREAMLIT
# =====================================================

init_db()

st.title("📊 Sistema Clientes — Novo / Retorno / Churn")

ref_date = st.sidebar.date_input(
    "Data fechamento",
    value=date.today()
)

uploaded = st.file_uploader(
    "Upload CSV mensal",
    type=["csv"]
)



   if uploaded:

    # leitura robusta CSV Brasil
    try:
        df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        try:
            df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="cp1252")
        except UnicodeDecodeError:
            df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="latin1")

    df = normalize_input(df_raw)

    inserted = upsert_pedidos(df)

    st.success(f"{inserted} pedidos adicionados")
try:
    df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8")
except UnicodeDecodeError:
    try:
        df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="cp1252")
    except UnicodeDecodeError:
        df_raw = pd.read_csv(uploaded, sep=None, engine="python", encoding="latin1")

    df = normalize_input(df_raw)

    inserted = upsert_pedidos(df)

    st.success(f"{inserted} pedidos adicionados")

st.divider()

base = build_cliente_base(ref_date)

if not base.empty:

    c1, c2, c3 = st.columns(3)

    c1.metric("Clientes", len(base))
    c2.metric("Ativos",
              (base["status"]=="Ativo").sum())
    c3.metric("Churn",
              (base["status"]=="Churn").sum())

    st.dataframe(base)

    st.download_button(
        "Baixar Base Clientes",
        base.to_csv(index=False),
        "base_clientes.csv"
    )

st.divider()

mes = st.text_input(
    "Mês Report (YYYY-MM)",
    value=ref_date.strftime("%Y-%m")
)

rep = month_report(mes)

if not rep.empty:
    st.dataframe(rep)
