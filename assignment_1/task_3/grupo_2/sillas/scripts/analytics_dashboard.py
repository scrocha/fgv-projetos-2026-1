from __future__ import annotations

import awswrangler as wr
import boto3
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from common import athena_output_s3, require_env, sql_text
from IPython.display import display

SQL_FILES = {
    "dim_products": "dim_products.sql",
    "sales_by_country": "sales_by_country.sql",
    "detailed_sales": "detailed_sales.sql",
}


def build_session() -> boto3.Session:
    return boto3.Session(region_name=require_env("AWS_REGION"))


def run_query(sql: str, database: str, session: boto3.Session) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql,
        database=database,
        boto3_session=session,
        workgroup=require_env("ATHENA_WORKGROUP"),
        s3_output=athena_output_s3(),
        ctas_approach=False,
    )


def query_dim_products(database: str, session: boto3.Session) -> pd.DataFrame:
    return run_query(sql_text(SQL_FILES["dim_products"]), database, session)


def query_sales_by_country(
    database: str, session: boto3.Session
) -> pd.DataFrame:
    return run_query(
        sql_text(SQL_FILES["sales_by_country"]), database, session
    )


def query_detailed_sales(
    database: str, session: boto3.Session
) -> pd.DataFrame:
    df = run_query(sql_text(SQL_FILES["detailed_sales"]), database, session)
    df["full_date"] = pd.to_datetime(df["full_date"])
    df["total_sales"] = pd.to_numeric(df["total_sales"])
    return df


def _filter_options(values: pd.Series) -> list[str]:
    return ["Todos", *sorted(value for value in values.dropna().unique())]


def build_dashboard(detail_df: pd.DataFrame) -> widgets.VBox:
    if detail_df.empty:
        raise RuntimeError(
            "A consulta detalhada nao retornou dados para montar o dashboard"
        )

    sns.set_theme(style="whitegrid")

    min_date = detail_df["full_date"].min().date()
    max_date = detail_df["full_date"].max().date()

    start_date = widgets.DatePicker(description="Inicio", value=min_date)
    end_date = widgets.DatePicker(description="Fim", value=max_date)
    country = widgets.Dropdown(
        description="Pais",
        options=_filter_options(detail_df["country"]),
        value="Todos",
    )
    product_line = widgets.Dropdown(
        description="Linha",
        options=_filter_options(detail_df["product_line"]),
        value="Todos",
    )
    top_n = widgets.IntSlider(
        description="Top N", min=1, max=10, step=1, value=10
    )
    output = widgets.Output()

    def render(*_) -> None:
        filtered = detail_df.copy()
        filtered = filtered[
            (filtered["full_date"].dt.date >= start_date.value)
            & (filtered["full_date"].dt.date <= end_date.value)
        ]

        if country.value != "Todos":
            filtered = filtered[filtered["country"] == country.value]
        if product_line.value != "Todos":
            filtered = filtered[filtered["product_line"] == product_line.value]

        ranked = (
            filtered.groupby("product_name", as_index=False)["total_sales"]
            .sum()
            .sort_values("total_sales", ascending=False)
            .head(top_n.value)
            .sort_values("total_sales", ascending=True)
        )

        with output:
            output.clear_output(wait=True)

            if ranked.empty:
                print("Nenhum dado encontrado para os filtros selecionados.")
                return

            fig, ax = plt.subplots(figsize=(10, 6))

            sns.barplot(
                data=ranked,
                x="total_sales",
                y="product_name",
                palette="Blues_r",
                ax=ax,
            )
            ax.set_xlabel("Vendas Totais")
            ax.set_ylabel("Produto")
            ax.set_title("Top produtos por vendas")
            plt.tight_layout()
            display(fig)
            plt.close(fig)

    for widget in (start_date, end_date, country, product_line, top_n):
        widget.observe(render, names="value")

    render()

    controls = widgets.HBox(
        [start_date, end_date, country, product_line, top_n]
    )
    return widgets.VBox([controls, output])
