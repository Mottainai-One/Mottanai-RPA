# Carga operacional → analítico às 02:00

O workflow `operational-to-analytics.yml` executa `run_analytics.py` diariamente
às **02:00 em America/Sao_Paulo** (`05:00 UTC`). A expressão usa UTC explícito
porque as execuções observadas da configuração anterior com `timezone` iniciaram
perto de 08:00 em São Paulo, depois do job Databricks das 02:30. Ele é
independente da sincronização legado → operacional. O GitHub agenda workflows
somente a partir da branch padrão; esta rotina começa após o merge da PR e a
configuração dos segredos.

## Preparar o banco analítico

No pgAdmin, conectado a `MOTTAINAI_ANALITICO`, aplique
`database/analytics/14_publish_mart_view.sql` do repositório
`Mottainai-Banco-Operacional`. A migração é repetível. Ela faz
`vw_published_batch` apontar para o `job_log` gravado na **mesma transação**
das dimensões, fatos e atualização de `mv_dashboard_metrics`.

O carregador exige a marca `publication_contract=analytics_mart_v1` na view e
falha antes de escrever caso a migração esteja ausente. Lotes antigos em
`etl_daily_batch` continuam para auditoria, mas não autorizam a publicação do
data mart quando os fatos estão vazios.

## Segredos do GitHub

Abra **Settings → Secrets and variables → Actions → New repository secret** em
<https://github.com/Mottainai-One/Mottanai-RPA/settings/secrets/actions>.
Configure os dez segredos abaixo; não coloque credenciais no código ou na PR.

| Origem operacional | Destino analítico |
| --- | --- |
| `OPERATIONAL_DB_HOST` | `ANALYTICS_DB_HOST` |
| `OPERATIONAL_DB_PORT` | `ANALYTICS_DB_PORT` |
| `OPERATIONAL_DB_NAME` = `MOTTAINAI` | `ANALYTICS_DB_NAME` = `MOTTAINAI_ANALITICO` |
| `OPERATIONAL_DB_USER` | `ANALYTICS_DB_USER` |
| `OPERATIONAL_DB_PASSWORD` | `ANALYTICS_DB_PASSWORD` |

Os dois bancos estão no serviço Aiven já usado pelo projeto. Confira host e
porta na console da Aiven antes de salvá-los. Use uma conta de leitura no
operacional e uma conta com escrita no analítico. O workflow usa SSL.

## Primeira execução e verificação

Após aplicar a migração, cadastrar os segredos e mesclar a PR, abra
<https://github.com/Mottainai-One/Mottanai-RPA/actions/workflows/operational-to-analytics.yml>
e escolha **Run workflow** para uma carga inicial. Confira o resultado no
pgAdmin conectado a `MOTTAINAI_ANALITICO`:

```sql
SELECT job_id, batch_id, cutoff_at, finished_at, rows_read, rows_written,
       metadata ->> 'publication_contract' AS publication_contract
FROM mottainai_analytics.job_log
WHERE job_name = 'operational_to_analytics_daily'
ORDER BY job_id DESC LIMIT 3;

SELECT count(*) FROM mottainai_analytics.fact_sales;
SELECT count(*) FROM mottainai_analytics.fact_inventory_snapshot;
SELECT * FROM mottainai_analytics.vw_published_batch;
```

A execução só publica o `batch_id` quando as contagens de cada destino batem
com a extração. Uma falha reverte a transação inteira. Depois, confira as três
views no catálogo estrangeiro `mottainai_analitico` do Databricks e execute
manualmente o job Bronze/Silver/Gold antes de habilitar sua agenda.

GitHub Actions pode iniciar uma execução agendada depois das 02:00 em períodos
de fila. `cutoff_at` registra o início real da leitura do operacional, de forma
que o dashboard mostre a idade correta dos dados.
