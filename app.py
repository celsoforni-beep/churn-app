import streamlit as st
import pandas as pd
import hashlib
import os
from datetime import date
import psycopg2
from psycopg2.extras import execute_values

# =====================================================
# CONFIG
# =====================================================

st.set_page_config(
    page_title="Sistema Clientes — Base 2025 + Análise 2026",
    layout="wide"
)

SALT = os.getenv("HASH_SALT", "default_salt")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# =====================================================
# HASH
# =====================================================

def clean_document(doc) -> str:
    return "".join([c for c in str(doc) if c.isdigit()])


def normalize_email(email) -> str:
    return str(email).strip().lower()


def hash_id(value: str) -> str:
    raw = (SALT + value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

# =====================================================
# DATABASE
# =====================================================

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurado no Secrets.")
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("select 1;")
    conn.commit()
    conn.close()

# =====================================================
# HELPERS
# =====================================================

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return df


def load_uploaded_file(uploaded) -> pd.DataFrame:
    filename = uploaded.name.lower()

    if filename.endswith(".xlsx"):
        df = pd.read_excel(uploaded)
        return normalize_headers(df)

    try:
        df = pd.read_csv(uploaded, sep=None, engine="python", encoding="utf-8")
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(uploaded, sep=None, engine="python", encoding="cp1252")
        except UnicodeDecodeError:
            df = pd.read_csv(uploaded, sep=None, engine="python", encoding="latin1")

    return normalize_headers(df)


def normalize_value_series(series: pd.Series) -> pd.Series:
    # Se já vier numérico, mantém
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    valor = series.astype(str).str.strip()

    # remove símbolo e espaços
    valor = valor.str.replace("R$", "", regex=False).str.strip()

    # formato BR: 1.120,96
    mask_br = valor.str.contains(",", na=False)

    valor_br = (
        valor[mask_br]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    # formato decimal normal: 1120.96
    valor_std = valor[~mask_br]

    valor_final = pd.concat([valor_br, valor_std]).sort_index()

    return pd.to_numeric(valor_final, errors="coerce")


def pick_column(cols_map, candidates):
    for k in candidates:
        if k in cols_map:
            return cols_map[k]
    return None

# =====================================================
# NORMALIZAÇÃO INPUT
# =====================================================

def normalize_input(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """
    mode:
      - 'base_2025'
      - 'mensal_2026'
    """
    df = normalize_headers(df)
    cols = {c.strip().lower(): c for c in df.columns}

    seller_candidates = ["seller", "marca", "brand"]
    doc_candidates = ["client document", "client_document", "clientdocument", "document", "cpf", "documento"]
    email_candidates = ["email", "e-mail", "mail"]
    order_candidates = ["order", "order2", "order 2", "order_id", "orderid", "pedido", "pedido id", "pedidoid"]
    date_candidates = ["creation d", "creation date", "created at", "data", "data pedido", "data_pedido", "creationd"]
    value_candidates = ["total value", "totalvalue", "total", "valor", "valor total", "total_value"]
    media_candidates = ["midia", "media", "source", "utm_source", "canal"]
    coupon_candidates = ["cupom", "coupon", "cupom_code", "coupon_code"]

    seller_col = pick_column(cols, seller_candidates)
    doc_col = pick_column(cols, doc_candidates)
    email_col = pick_column(cols, email_candidates)
    order_col = pick_column(cols, order_candidates)
    date_col = pick_column(cols, date_candidates)
    value_col = pick_column(cols, value_candidates)
    media_col = pick_column(cols, media_candidates)
    coupon_col = pick_column(cols, coupon_candidates)

    if seller_col is None:
        raise ValueError(f"Não achei coluna de marca. Colunas detectadas: {list(cols.keys())}")
    if order_col is None:
        raise ValueError(f"Não achei coluna de pedido. Colunas detectadas: {list(cols.keys())}")
    if date_col is None:
        raise ValueError(f"Não achei coluna de data. Colunas detectadas: {list(cols.keys())}")
    if (doc_col is None) and (email_col is None):
        raise ValueError("Não achei Client Document nem Email. Preciso de pelo menos 1 identificador.")

    base = df[[seller_col, order_col, date_col]].copy()
    base.columns = ["marca", "order_id", "data_pedido"]

    base["client_document"] = df[doc_col] if doc_col else ""
    base["email"] = df[email_col] if email_col else ""

    base["marca"] = base["marca"].astype(str).str.strip().str.upper()
    base["order_id"] = base["order_id"].astype(str).str.strip()

    base["data_pedido"] = pd.to_datetime(base["data_pedido"], dayfirst=True, errors="coerce")
    base = base.dropna(subset=["data_pedido", "order_id", "marca"])

    doc_clean = base["client_document"].apply(clean_document)
    email_clean = base["email"].apply(normalize_email)

    identificador = doc_clean.copy()
    mask_sem_doc = identificador.str.len() == 0
    identificador.loc[mask_sem_doc] = email_clean.loc[mask_sem_doc]

    base["customer_id"] = identificador.apply(lambda x: hash_id(str(x)))
    base["mes_compra"] = base["data_pedido"].dt.strftime("%Y-%m")

    if value_col:
        base["valor_pedido"] = normalize_value_series(df[value_col])
    else:
        base["valor_pedido"] = None

    if mode == "mensal_2026":
        base["midia"] = df[media_col].astype(str).str.strip() if media_col else "SEM_MIDIA"
        base["cupom"] = df[coupon_col].astype(str).str.strip() if coupon_col else "SEM_CUPOM"

        base.loc[base["midia"].isin(["", "nan", "None"]), "midia"] = "SEM_MIDIA"
        base.loc[base["cupom"].isin(["", "nan", "None"]), "cupom"] = "SEM_CUPOM"

        return base[[
            "customer_id", "marca", "order_id", "data_pedido",
            "mes_compra", "valor_pedido", "midia", "cupom"
        ]]

    return base[[
        "customer_id", "marca", "order_id", "data_pedido",
        "mes_compra", "valor_pedido"
    ]]

# =====================================================
# UPSERTS
# =====================================================

def ensure_2026_columns():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("alter table public.pedidos add column if not exists midia text;")
    cur.execute("alter table public.pedidos add column if not exists cupom text;")
    conn.commit()
    conn.close()


def upsert_pedidos_2025(df: pd.DataFrame) -> int:
    conn = get_conn()
    cur = conn.cursor()

    before = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]

    df2 = df.copy()
    df2["data_pedido"] = pd.to_datetime(df2["data_pedido"]).dt.date

    rows = list(
        df2[["customer_id", "marca", "order_id", "data_pedido", "mes_compra", "valor_pedido"]]
        .itertuples(index=False, name=None)
    )

    execute_values(
        cur,
        """
        insert into public.pedidos (customer_id, marca, order_id, data_pedido, mes_compra, valor_pedido)
        values %s
        on conflict (order_id, marca) do update set
          customer_id = excluded.customer_id,
          data_pedido = excluded.data_pedido,
          mes_compra = excluded.mes_compra,
          valor_pedido = excluded.valor_pedido
        """,
        rows,
        page_size=5000
    )

    conn.commit()
    after = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]
    conn.close()

    return int(after - before)


def upsert_pedidos_2026(df: pd.DataFrame) -> int:
    ensure_2026_columns()

    conn = get_conn()
    cur = conn.cursor()

    before = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]

    df2 = df.copy()
    df2["data_pedido"] = pd.to_datetime(df2["data_pedido"]).dt.date

    rows = list(
        df2[[
            "customer_id", "marca", "order_id", "data_pedido",
            "mes_compra", "valor_pedido", "midia", "cupom"
        ]].itertuples(index=False, name=None)
    )

    execute_values(
        cur,
        """
        insert into public.pedidos
        (customer_id, marca, order_id, data_pedido, mes_compra, valor_pedido, midia, cupom)
        values %s
        on conflict (order_id, marca) do update set
          customer_id = excluded.customer_id,
          data_pedido = excluded.data_pedido,
          mes_compra = excluded.mes_compra,
          valor_pedido = excluded.valor_pedido,
          midia = excluded.midia,
          cupom = excluded.cupom
        """,
        rows,
        page_size=5000
    )

    conn.commit()
    after = pd.read_sql("select count(*) as n from public.pedidos", conn)["n"][0]
    conn.close()

    return int(after - before)

# =====================================================
# ANALYTICS 2025
# =====================================================

@st.cache_data(ttl=600)
def build_cliente_base_2025() -> pd.DataFrame:
    conn = get_conn()
    base = pd.read_sql("select * from public.clientes_base_2025", conn)
    conn.close()
    return base


@st.cache_data(ttl=600)
def resumo_base_2025() -> pd.DataFrame:
    base = build_cliente_base_2025()
    if base.empty:
        return pd.DataFrame()

    resumo = (
        base.groupby("marca")
        .agg(
            clientes=("customer_id", "count"),
            ativos=("status_2025", lambda x: (x == "Ativo").sum()),
            em_risco=("status_2025", lambda x: (x == "Em risco").sum()),
            churn=("status_2025", lambda x: (x == "Churn").sum()),
            receita_total=("receita_total", "sum")
        )
        .reset_index()
    )
    return resumo

# =====================================================
# ANALYTICS 2026
# =====================================================

@st.cache_data(ttl=300)
def analisar_mes_2026(mes_ref: str, marca_ref: str = "TODAS"):
    conn = get_conn()

    query_mes = f"""
    select
        p.customer_id,
        p.marca,
        p.order_id,
        p.data_pedido,
        p.mes_compra,
        p.valor_pedido,
        coalesce(p.midia, 'SEM_MIDIA') as midia,
        coalesce(p.cupom, 'SEM_CUPOM') as cupom,
        b.status_2025,
        case
            when b.customer_id is null then 'Novo'
            when b.status_2025 = 'Churn' then 'Recuperado_Churn_2025'
            else 'Retorno'
        end as classificacao_2026
    from public.pedidos p
    left join public.clientes_base_2025 b
      on p.customer_id = b.customer_id
     and p.marca = b.marca
    where p.mes_compra = %(mes_ref)s
      and p.data_pedido >= date '2026-01-01'
    """

    params = {"mes_ref": mes_ref}
    df = pd.read_sql(query_mes, conn, params=params)
    conn.close()

    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if marca_ref != "TODAS":
        df = df[df["marca"] == marca_ref].copy()

    resumo = pd.DataFrame([{
        "Mes": mes_ref,
        "Marca": marca_ref,
        "Clientes_Mes": int(df["customer_id"].nunique()),
        "Pedidos_Mes": int(df["order_id"].nunique()),
        "Receita_Mes": float(df["valor_pedido"].sum(skipna=True)),
        "Novos": int(df.loc[df["classificacao_2026"] == "Novo", "customer_id"].nunique()),
        "Retorno": int(df.loc[df["classificacao_2026"] == "Retorno", "customer_id"].nunique()),
        "Recuperados_Churn_2025": int(df.loc[df["classificacao_2026"] == "Recuperado_Churn_2025", "customer_id"].nunique())
    }])

    midia = (
        df.groupby(["midia", "classificacao_2026"])
        .agg(
            clientes=("customer_id", "nunique"),
            pedidos=("order_id", "nunique"),
            receita=("valor_pedido", "sum")
        )
        .reset_index()
        .sort_values(["midia", "classificacao_2026"])
    )

    cupom = (
        df.groupby(["cupom", "classificacao_2026"])
        .agg(
            clientes=("customer_id", "nunique"),
            pedidos=("order_id", "nunique"),
            receita=("valor_pedido", "sum")
        )
        .reset_index()
        .sort_values(["cupom", "classificacao_2026"])
    )

    detalhe = (
        df.groupby(["marca", "classificacao_2026"])
        .agg(
            clientes=("customer_id", "nunique"),
            pedidos=("order_id", "nunique"),
            receita=("valor_pedido", "sum")
        )
        .reset_index()
        .sort_values(["marca", "classificacao_2026"])
    )

    return resumo, midia, cupom, detalhe

# =====================================================
# APP
# =====================================================

try:
    init_db()
except Exception as e:
    st.error("Falha ao conectar no Supabase. Verifique DATABASE_URL no Secrets.")
    st.exception(e)
    st.stop()

st.title("📊 Sistema Clientes — Base 2025 + Análise Mensal 2026")

# -----------------------------------------------------
# BLOCO 1 - BASE 2025
# -----------------------------------------------------

st.header("1. Base congelada 2025")

resumo_2025 = resumo_base_2025()

if resumo_2025.empty:
    st.warning("A tabela clientes_base_2025 está vazia ou não foi criada.")
else:
    st.subheader("Resumo base 2025 por marca")
    st.dataframe(resumo_2025, use_container_width=True)

    base2025 = build_cliente_base_2025()
    st.download_button(
        "Baixar clientes_base_2025.csv",
        data=base2025.to_csv(index=False).encode("utf-8"),
        file_name="clientes_base_2025.csv",
        mime="text/csv"
    )

st.divider()

# -----------------------------------------------------
# BLOCO 2 - UPLOAD MENSAL 2026
# -----------------------------------------------------

st.header("2. Upload mensal 2026")

uploaded_2026 = st.file_uploader(
    "Upload mensal 2026 (Excel .xlsx ou CSV)",
    type=["xlsx", "csv"],
    key="upload_2026"
)

if uploaded_2026 is not None:
    try:
        df_raw_2026 = load_uploaded_file(uploaded_2026)
        st.write("✅ Colunas detectadas:", list(df_raw_2026.columns))

        df_2026 = normalize_input(df_raw_2026, mode="mensal_2026")
        inserted_2026 = upsert_pedidos_2026(df_2026)

        analisar_mes_2026.clear()

        st.success(f"{inserted_2026} pedidos adicionados / atualizados em 2026")

    except Exception as e:
        st.error("Erro ao processar o arquivo mensal de 2026.")
        st.exception(e)

st.divider()

# -----------------------------------------------------
# BLOCO 3 - ANÁLISE MENSAL 2026
# -----------------------------------------------------

st.header("3. Análise mensal 2026")

col1, col2 = st.columns(2)

with col1:
    mes_ref = st.text_input("Mês de análise (YYYY-MM)", value="2026-01")

with col2:
    marca_ref = st.selectbox(
        "Marca",
        options=["TODAS", "MIZ", "OLYMPIKUS", "UA"],
        index=0
    )

resumo_mes, ranking_midia, ranking_cupom, detalhe_marca = analisar_mes_2026(mes_ref, marca_ref)

if resumo_mes.empty:
    st.info("Sem dados para esse mês / marca.")
else:
    st.subheader("Resumo executivo do mês")
    st.dataframe(resumo_mes, use_container_width=True)

    st.subheader("Ranking por mídia")
    st.dataframe(ranking_midia, use_container_width=True)

    st.subheader("Ranking por cupom")
    st.dataframe(ranking_cupom, use_container_width=True)

    st.subheader("Resumo por classificação")
    st.dataframe(detalhe_marca, use_container_width=True)

    st.download_button(
        "Baixar resumo_mes_2026.csv",
        data=resumo_mes.to_csv(index=False).encode("utf-8"),
        file_name=f"resumo_{mes_ref}.csv",
        mime="text/csv"
    )

    st.download_button(
        "Baixar ranking_midia_2026.csv",
        data=ranking_midia.to_csv(index=False).encode("utf-8"),
        file_name=f"ranking_midia_{mes_ref}.csv",
        mime="text/csv"
    )

    st.download_button(
        "Baixar ranking_cupom_2026.csv",
        data=ranking_cupom.to_csv(index=False).encode("utf-8"),
        file_name=f"ranking_cupom_{mes_ref}.csv",
        mime="text/csv"
    )
