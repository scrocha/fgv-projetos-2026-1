# Task 3 - Grupo 2 / Sillas

Implementacao da task 3 baseada na entrega da task 2, sem alterar a infraestrutura ja criada.

Esta solucao faz tres coisas:

- provisiona com Terraform apenas o workgroup do Athena da task 3;
- registra no Glue Data Catalog/Athena as tabelas Parquet produzidas pela task 2;
- valida as consultas analiticas minimas pedidas no enunciado;
- entrega um notebook Jupyter com dashboard interativo em Python usando `awswrangler`, `pandas`, `seaborn` e `ipywidgets`.

## Estrutura

- `sql/`: consultas Athena usadas na validacao e no notebook
- `terraform/`: cria o workgroup do Athena
- `scripts/common.py`: configuracao compartilhada e resolucao de variaveis
- `scripts/setup_athena.py`: garante o database e registra as tabelas externas dinamicamente no Glue/Athena
- `scripts/validate_task3.py`: executa e valida as tres consultas minimas
- `scripts/run_task3.py`: orquestra Terraform + setup + validacao, no mesmo estilo da task 2
- `scripts/analytics_dashboard.py`: funcoes reutilizaveis para o notebook/dashboard
- `notebooks/task3_dashboard.ipynb`: notebook interativo da task 3

## Setup necessario

- A `task_2` do grupo 2 ja foi aplicada e produziu os Parquets em `s3://<bucket>/analytics/...`
- As credenciais AWS estao configuradas localmente
- `uv` esta instalado

## Dependencia da Task 2

Antes de executar a `task_3`, e necessario executar a `task_2` com sucesso.

Isso e necessario porque a implementacao da `task_3` nao recria o pipeline ETL nem reprovisiona os dados de origem. Ela apenas:

- reaproveita o bucket e os arquivos Parquet gerados pela `task_2`;
- usa Terraform para provisionar o workgroup da camada analitica;
- consulta o output do Terraform da `task_2` para descobrir o bucket quando `DATA_LAKE_BUCKET` nao e informado;
- garante via Python o database e registra tabelas externas apontando para os dados ja materializados pela `task_2`.

Em outras palavras: a `task_2` produz os dados analiticos, e a `task_3` consome esses dados.

Por padrao, os scripts tentam descobrir automaticamente o bucket da task 2 via `terraform output` em `assignment_1/task_2/grupo_2/final/terraform`.

## Configuracao

Crie um `.env` a partir de `.env.example`.

Variaveis mais importantes:

- `AWS_REGION`
- `GLUE_DATABASE`
- `DATA_LAKE_BUCKET`
- `ATHENA_OUTPUT_S3`
- `ATHENA_WORKGROUP`
- `ATHENA_RESULTS_PREFIX`

Se `DATA_LAKE_BUCKET` estiver vazio, a task 3 tenta usar o output `analytics_bucket_name` da task 2.
Se `GLUE_DATABASE`, `ATHENA_WORKGROUP` ou `ATHENA_OUTPUT_S3` estiverem vazios, a task 3 prioriza os outputs do Terraform local da propria task 3.
Se `ATHENA_RESULTS_PREFIX` nao for informado, o Terraform da task 3 usa `athena-results/`.

O script `scripts/run_task3.py` passa automaticamente essas variaveis ao Terraform via `TF_VAR_*`, entao normalmente nao e preciso preencher `terraform.tfvars` manualmente.

## Ordem de execucao

Se a `task_2` ainda nao tiver sido executada neste ambiente, rode antes:

```bash
cd assignment_1/task_2/grupo_2/final
uv sync
uv run python scripts/run_pipeline.py
```

Depois execute a `task_3`:

```bash
cd assignment_1/task_3/grupo_2/sillas
uv sync
uv run python scripts/run_task3.py
```

## Execucao por etapas

```bash
cd assignment_1/task_3/grupo_2/sillas
uv sync
terraform -chdir=terraform init
terraform -chdir=terraform apply
uv run python scripts/setup_athena.py
uv run python scripts/validate_task3.py
```

## Notebook

Depois de registrar as tabelas e validar as consultas:

Abra `notebooks/task3_dashboard.ipynb`.

## O que a task 3 cria e usa

- Terraform cria:
- Athena workgroup com local de resultados no S3
- Python registra as tabelas externas:
  - garante o database no Glue
  - `fact_orders`
  - `dim_customers`
  - `dim_products`
  - `dim_dates`
  - `dim_countries`
