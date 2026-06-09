# Assignment 2 - Task 2: ETL incremental

Implementacao simples e reexecutavel da Task 2 do Grupo 2 / Sillas.

Esta solucao evolui o pipeline do Assignment 1 para:

- ler apenas novos pedidos do RDS MySQL usando `etl_watermark`;
- executar ETL incremental no AWS Glue via JDBC;
- gravar `fact_orders` particionado por `order_year` e `order_month`;
- registrar tabelas no Glue Catalog para Athena;
- declarar a agenda em Terraform e usar Glue Trigger agendado no lab de estudante.

## Estrutura

- `terraform/`: infraestrutura AWS, Glue Catalog e agendamento
- `glue/etl_job.py`: job Glue PySpark incremental
- `scripts/common.py`: configuracao compartilhada e fallbacks por `terraform output`
- `scripts/load_classicmodels.py`: carga historica do banco `classicmodels`
- `scripts/init_watermark.py`: cria/inicializa `etl_watermark`
- `scripts/simulate_new_orders.py`: simula novos pedidos no OLTP
- `scripts/validate_incremental_source.py`: valida dados pendentes na origem
- `scripts/run_glue_job.py`: dispara e acompanha o Glue Job
- `scripts/validate_pipeline.py`: valida S3, particoes, watermark e regra `sales_amount`
- `scripts/run_pipeline.py`: orquestrador ponta a ponta

## Pre-requisitos

- AWS credentials disponiveis para Terraform e `boto3`
- Terraform instalado
- `uv` instalado
- Permissoes do laboratorio para RDS, S3, Glue, Glue Catalog, EventBridge e IAM

Arquivos `.env` e `terraform/terraform.tfvars` sao opcionais. Se nao existirem, os scripts usam defaults e outputs do Terraform. A senha do RDS (`DB_PASSWORD`) e gerada automaticamente em `.runtime.env` quando nao for definida pelo usuario.

## Ordem de execucao

```bash
cd assignment_2/task_2/grupo_2/sillas
uv sync
uv run python scripts/run_pipeline.py --dry-run
uv run python scripts/run_pipeline.py
```

O orquestrador executa:

```text
1. Terraform init/plan/apply
2. carga historica do classicmodels
3. inicializacao do watermark
4. simulacao de novos pedidos
5. Glue Job incremental
6. validacao final
```

## Configuracao opcional

Use `.env` apenas se quiser fixar credenciais ou nomes manualmente:

```bash
AWS_REGION=us-east-1
DB_HOST=
DB_PORT=3306
DB_NAME=classicmodels
DB_USER=admin
DB_PASSWORD=<senha-do-rds>
GLUE_JOB_NAME=classicmodels-etl-job
S3_BUCKET_NAME=
```

Use `terraform/terraform.tfvars` apenas se quiser sobrescrever parametros de infraestrutura:

```hcl
bucket_name                     = ""
eventbridge_schedule_expression = "cron(0 12 ? * MON *)"
existing_glue_role_name         = "LabRole"
```

Nunca commite `.env`, `.runtime.env` ou `terraform/terraform.tfvars`. Estes arquivos ja estao no `.gitignore`.

## Execucao manual alternativa

Use somente se quiser rodar por etapas:

```bash
cd assignment_2/task_2/grupo_2/sillas
uv sync
export DB_PASSWORD="${DB_PASSWORD:-$(uv run python -c 'from scripts.common import generate_password; print(generate_password())')}"
export TF_VAR_db_password="$DB_PASSWORD"
terraform -chdir=terraform init
terraform -chdir=terraform apply
uv run python scripts/load_classicmodels.py
uv run python scripts/init_watermark.py
uv run python scripts/simulate_new_orders.py --count 5 --seed 42
uv run python scripts/validate_incremental_source.py
uv run python scripts/run_glue_job.py
uv run python scripts/validate_pipeline.py
```

## Segunda rodada incremental

Depois da primeira execucao bem-sucedida, rode uma segunda simulacao para evidenciar que apenas pedidos acima do watermark anterior sao extraidos:

```bash
uv run python scripts/simulate_new_orders.py --count 3 --seed 202
uv run python scripts/validate_incremental_source.py
uv run python scripts/run_glue_job.py
uv run python scripts/validate_pipeline.py
```

Registre o Job Run ID impresso por `scripts/run_glue_job.py`.

## O que a Task 2 cria

Terraform cria ou configura:

- RDS MySQL para `classicmodels`
- bucket S3 de analytics
- Glue Connection JDBC
- Glue Job incremental
- Glue Catalog database e tabelas externas
- `fact_orders` com partition keys `order_year` e `order_month`
- EventBridge rule semanal (`cron(0 12 ? * MON *)`)
- Glue Trigger agendado com o mesmo cron para iniciar o Glue Job no lab
- EventBridge target direto desabilitado por default, porque `PutTargets` rejeita Glue Job ARN nesse ambiente

Por padrao, o Glue usa a role existente `LabRole`, adequada para o lab de estudante. A configuracao default evita `iam:CreateRole`.

Se sua conta permitir criar roles proprias, ajuste:

```hcl
create_glue_role        = true
create_eventbridge_role = true
```

Se sua conta aceitar Glue Job como alvo direto do EventBridge, ajuste também:

```hcl
enable_eventbridge_target = true
```

No lab atual, o alvo direto falha com `Provided Arn is not in correct format`; por isso o agendamento funcional fica em `aws_glue_trigger`.

## Saidas esperadas

Dimensoes:

```text
s3://<bucket>/analytics/dim_customers/
s3://<bucket>/analytics/dim_products/
s3://<bucket>/analytics/dim_dates/
s3://<bucket>/analytics/dim_countries/
```

Fato particionada:

```text
s3://<bucket>/analytics/fact_orders/order_year=YYYY/order_month=M/part-....parquet
```

## Athena

Database default:

```text
classicmodels_analytics
```

Consulta de validacao:

```sql
SELECT COUNT(*)
FROM classicmodels_analytics.fact_orders
WHERE order_year = 2026
  AND order_month = 6;
```

A tabela usa partition projection; nao precisa crawler nem `MSCK REPAIR TABLE`.

## Evidencia recomendada

Registre depois de executar na AWS:

```text
Rodada 1
- watermark antes=<YYYY-MM-DD>
- pedidos simulados=<N>
- Glue Job Run ID=<jr_...>
- watermark depois=<YYYY-MM-DD>
- novas linhas fact_orders=<linhas orderdetails simuladas>

Rodada 2
- watermark antes=<YYYY-MM-DD>
- pedidos simulados=<N>
- Glue Job Run ID=<jr_...>
- watermark depois=<YYYY-MM-DD>
- apenas orderDate > watermark anterior foi extraido

EventBridge
- rule=<eventbridge_rule_name>
- glue_trigger=<glue_schedule_trigger_name>
- Glue Job Run ID disparado pela agenda/trigger=<jr_...>
```

## Limpeza opcional

```bash
terraform -chdir=terraform destroy
```
