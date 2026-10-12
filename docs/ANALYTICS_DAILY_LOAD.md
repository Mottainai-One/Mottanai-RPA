# Carga diária do banco analítico

## Objetivo

O RPA legado → operacional pode rodar independentemente. Este segundo fluxo lê o estado do banco operacional e atualiza o banco analítico uma vez por dia, às **02:00 no fuso America/Sao_Paulo**. O agendamento fica em `dags/operational_to_analytics.py`. Também é possível executar manualmente com `python run_analytics.py`.

## Preparação

1. Instale os scripts do schema `mottainai` no banco operacional `MOTTAINAI` e os scripts de `database/analytics/install.sql` no banco físico `MOTTAINAI_ANALITICO`. No pgAdmin, use `install_pgadmin.sql` do repositório do banco para instalar sem comandos do psql.
2. Copie `.env.example` para `.env` e preencha `OPERATIONAL_DB_*` e `ANALYTICS_DB_*`. Use contas distintas: leitura no operacional e escrita no analítico.
3. Instale `requirements.txt`. Para agendamento, instale e configure Airflow separadamente e inclua este repositório no `PYTHONPATH` dos workers.
4. Se houver histórico no antigo schema `MOTTAINAI.analytics`, execute `python -m analytics.bootstrap_history` uma vez. Execute `python run_analytics.py` manualmente e confira as contagens. Depois habilite o agendamento escolhido.

## Dados carregados

| Origem operacional | Destino analítico | Observação |
| --- | --- | --- |
| `company`, `retail_store`, `product_category`, `product`, `employee_role`, `employee` e `customer` | `dim_company`, `dim_store`, `dim_category`, `dim_product`, `dim_employee`, `dim_customer` | Sem dados pessoais ou credenciais; cliente identificado só por chave anônima gerada no analítico |
| `sales_transaction`, `sale_item`, `sale_payment` | `fact_sales`, `fact_sale_item`, `fact_sale_payment` | Venda e itens cancelados preservam o status da venda; apenas itens vendidos entram em `fact_sale_item` |
| `inventory_movement`, `inventory`, `batch` | `fact_inventory_movement`, `fact_inventory_snapshot` | Snapshot recebe a data da execução em São Paulo |
| `retail_store`, `employee`, `app_user` | `fact_store_daily` | Resumo diário para as views de empresas e lojas |

O banco analítico possui outras tabelas de IA, fidelidade e sustentabilidade. Elas **não são carregadas por este fluxo** e só terão dados quando suas fontes operacionais forem mapeadas.

`dim_supplier` também aguarda ajuste de contrato: o destino exige `company_id`, mas `supplier` no operacional não tem esse vínculo obrigatório. A carga não inventa uma empresa para o fornecedor.

## Reexecução e consistência

- A primeira execução carrega todos os registros das entidades acima. Cada execução seguinte reconcilia o histórico inteiro: dimensões usam `ON CONFLICT DO UPDATE` e fatos transacionais são substituídos dentro da transação. Isso captura correções e cancelamentos tardios mesmo porque `updated_at` não é atualizado automaticamente em todas as tabelas operacionais. É uma solução para a massa atual; antes de aumentar muito o volume, adicionar captura de alterações ou watermarks confiáveis.
- O snapshot de estoque da data da execução é substituído dentro da mesma transação. Se a carga falhar, a transação do analítico é revertida.
- A leitura usa uma transação `REPEATABLE READ`, mantendo uma visão consistente do operacional durante o lote. O processo grava a contagem por tabela no log e uma linha de sucesso em `mottainai_analytics.job_log`.
- A leitura do operacional termina antes de abrir a conexão de escrita no analítico, respeitando o limite de conexões do Aiven Free. Para o volume atual, as linhas são mantidas em memória entre essas etapas.
- Timestamps operacionais sem fuso são interpretados no destino como horários de São Paulo. Confirme que esse é o fuso usado pelo aplicativo antes da primeira carga real.
- O pipeline não copia CPF, CNPJ, nome de cliente, e-mail, telefone nem hashes de senha.

## Validação após a execução

```sql
SELECT job_id, job_name, status, started_at, finished_at, rows_read, rows_written
FROM mottainai_analytics.job_log
WHERE job_name = 'operational_to_analytics_daily'
ORDER BY job_id DESC LIMIT 5;

SELECT count(*) FROM mottainai_analytics.dim_company;
SELECT count(*) FROM mottainai_analytics.dim_store;
SELECT count(*) FROM mottainai_analytics.fact_sales;
SELECT count(*) FROM mottainai_analytics.fact_inventory_snapshot
WHERE snapshot_date = CURRENT_DATE;
SELECT count(*) FROM mottainai_analytics.fact_store_daily;
SELECT count(*) FROM mottainai_analytics.vw_company_kpis;
SELECT count(*) FROM mottainai_analytics.vw_employee_distribution;
SELECT count(*) FROM mottainai_analytics.vw_store_daily_kpis;
```

Compare essas contagens às tabelas operacionais. Se vendas e estoque ainda não foram usados pela aplicação, as respectivas tabelas fato podem estar vazias; isso não indica erro da carga.

## Limites atuais para os KPIs

O esquema operacional não fornece desconto por item nem quantidade reservada no mapeamento atual. A carga preenche esses campos com zero. Assim, métricas de descontos e disponibilidade reservada não devem ser apresentadas como valores reais até que suas fontes sejam definidas. O dashboard deve passar a consultar as três `vw_*` do banco físico `MOTTAINAI_ANALITICO`; a criação do job no Databricks ainda é uma entrega separada.
