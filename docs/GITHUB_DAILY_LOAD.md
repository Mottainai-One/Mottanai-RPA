# Carga operacional → analítico às 02:00

O workflow `operational-to-analytics.yml` executa `run_analytics.py` diariamente
às **02:00 em America/Sao_Paulo** (`05:00 UTC`). A expressão usa UTC explícito
e é independente da sincronização legado → operacional. O horário é uma meta
de disparo, não garantia de início pontual: o GitHub pode atrasar execuções
agendadas. O workflow só roda automaticamente depois de estar na branch padrão
e de os segredos estarem configurados.

## Preparar o banco analítico

No pgAdmin, conectado a `MOTTAINAI_ANALITICO`, aplique
`database/analytics/14_publish_mart_view.sql` do repositório
`Mottainai-Banco-Operacional`. A migração é repetível. Ela faz
`vw_published_batch` apontar para o `job_log` gravado na **mesma transação**
das dimensões, fatos e atualização de `mv_dashboard_metrics`.

O carregador exige a marca `publication_contract=analytics_mart_v1` na view e
falha antes de escrever caso a migração esteja ausente.

**Importante:** o workflow ativo executa `run_analytics.py` (`analytics.load`).
Esse carregador reconcilia dimensões e fatos diretamente, atualiza
`mv_dashboard_metrics` e grava um registro `SUCCEEDED` em `job_log` na mesma
transação. Ele não abre uma linha em `etl_daily_batch`. Portanto, uma tabela
`etl_daily_batch` vazia não significa que essa execução falhou; para esse
workflow, a evidência de publicação é `vw_published_batch` e o `job_log`.

`run_analytics_daily.py` é outro fluxo, baseado em staging e nas funções de
`etl_daily_batch`. Ele não é chamado por este workflow e não deve ser usado
como substituto até que a etapa de transformação do staging para os fatos do
data mart esteja implementada e validada. Não mantenha os dois agendadores
ativos para a mesma carga.

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

## Acionar o Databricks depois da publicação

Depois que `run_analytics.py` confirma a transação no analítico, o
workflow pode iniciar o job `mottainai-analytics-to-medallion` e aguardar o
resultado de Bronze → Silver → Gold → refresh do dashboard. A autenticação é
GitHub OIDC com um service principal do Databricks; nenhum token Databricks é
armazenado no GitHub. O acionamento só ocorre na branch `main`.

Configuração única necessária antes de habilitar o acionamento:

1. Crie um service principal no Databricks Account Console e adicione-o ao
   workspace `dbc-38ac8920-cbf4.cloud.databricks.com`.
2. No job `mottainai-analytics-to-medallion` (ID `731348243647869`), conceda a
   esse principal somente a permissão `CAN MANAGE RUN`.
3. No principal, crie uma política de federação de workload com:
   - Issuer: `https://token.actions.githubusercontent.com`
   - Audience: `6652101d-d790-4989-8073-d3d8f687c003`
   - Subject: `repo:Mottainai-One@310825313/Mottanai-RPA@1353721751:environment:databricks-production`
4. Em **Settings → Environments**, crie o ambiente `databricks-production`.
   Adicione estas **variables** ao ambiente (não são secrets):

   | Variable | Value |
   | --- | --- |
   | `DATABRICKS_HOST` | `https://dbc-38ac8920-cbf4.cloud.databricks.com` |
   | `DATABRICKS_CLIENT_ID` | Application ID do service principal |
   | `DATABRICKS_JOB_ID` | `731348243647869` |

Sem essas variables, o workflow emite um aviso e o agendamento Databricks das
02:30 permanece como fallback. Depois de validar o primeiro acionamento OIDC
end-to-end, pause o agendamento fixo do job Databricks para evitar uma segunda
execução sem lote novo. A carga do RPA segue agendada para 02:00 em São Paulo;
o job Databricks acionado pelo workflow executa assim que a publicação
analítica termina.

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

SELECT job_id, batch_id, source_cutoff_at, snapshot_date, published_at
FROM mottainai_analytics.vw_published_batch;

SELECT count(*) FROM mottainai_analytics.fact_sales;
SELECT count(*) FROM mottainai_analytics.fact_inventory_snapshot;
```

A execução publica o `batch_id` em `job_log` quando as contagens de cada
destino batem com a extração. Uma falha reverte a transação inteira. Com o OIDC
configurado, o mesmo workflow dispara o job Bronze/Silver/Gold e aguarda o
refresh do dashboard. Sem OIDC, o job Databricks das 02:30 é o fallback
automático.

O cron está configurado para 02:00 em São Paulo, mas o GitHub Actions pode
iniciar uma execução agendada com atraso ou não iniciá-la em períodos de alta
carga; o cron não é uma garantia de execução pontual. Além disso, workflows
agendados rodam a partir da branch padrão. `cutoff_at` registra o início real
da leitura do operacional, de forma que o dashboard mostre a idade correta dos
dados. Para exigir horário pontual, use um scheduler gerenciado com suporte a
timezone e monitore o resultado da execução.
