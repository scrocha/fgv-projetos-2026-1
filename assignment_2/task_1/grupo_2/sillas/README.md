# Assignment 2 - Task 1: Origem Incremental

Este diretório contém a preparação do banco de dados MySQL de origem (RDS) para suportar cargas incrementais.

## Estrutura de Arquivos

* `scripts/common.py`: Utilitários de conexão e fallbacks inteligentes de variáveis de ambiente.
* `scripts/bootstrap.py`: Script que executa automaticamente o setup de infraestrutura e dados do *Assignment 1*.
* `scripts/init_watermark.py`: Inicializa a tabela de metadados (`etl_watermark`) no banco de dados.
* `scripts/simulate_new_orders.py`: Insere novos pedidos de teste com datas e regras coerentes.
* `scripts/validate_incremental_source.py`: Valida se a origem e os metadados estão prontos para o ETL.

---

## 1. Setup Rápido (RDS do Assignment 1)

Esta tarefa requer uma instância RDS MySQL rodando a base `classicmodels` original. 

Se você possui credenciais da AWS configuradas localmente, você pode provisionar a infraestrutura e carregar a base de dados automaticamente executando:

```bash
cd assignment_2/task_1/grupo_2/sillas
uv sync
uv run python scripts/bootstrap.py
```

---

## 2. Fluxo de Trabalho (Task 1)

Após garantir que o banco está rodando e carregado com os dados históricos, execute os passos abaixo para preparar a origem incremental:

### Passo 1: Inicializar o Watermark
Cria a tabela de metadados e define a data máxima dos pedidos históricos como ponto de partida (baseline):
```bash
uv run python scripts/init_watermark.py
```

### Passo 2: Validar o Baseline (Opcional)
Verifica que o watermark foi criado com sucesso (nenhum pedido pendente é esperado neste momento):
```bash
uv run python scripts/validate_incremental_source.py
```

### Passo 3: Simular Chegada de Novos Pedidos
Adiciona novos pedidos com datas posteriores ao watermark e valorações coerentes:
```bash
uv run python scripts/simulate_new_orders.py --count 10
```

### Passo 4: Validar Carga Pendente
Valida que existem novos dados na origem pendentes de extração (o script deve retornar código de saída `0`):
```bash
uv run python scripts/validate_incremental_source.py
```