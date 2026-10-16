# Carga diaria do banco analitico

O job `mottainai_operational_to_analytics_daily` lê o banco operacional `MOTTAINAI` e publica um batch no schema `mottainai_analytics` diariamente às 02:00. A data de referência é o dia anterior no fuso `America/Sao_Paulo`. Ele não acessa o legado.

Teste local:

```powershell
$env:ANALYTICS_DRY_RUN="true"
.\.venv\Scripts\python.exe -u run_analytics_daily.py
```

O teste executa em transação e faz rollback. Para carga real manual, somente após validar:

```powershell
$env:ANALYTICS_DRY_RUN="false"
.\.venv\Scripts\python.exe -u run_analytics_daily.py
```

O DAG `dags/mottainai_analytics_daily.py` usa `0 2 * * *`, três retries, intervalo de dez minutos e uma execução ativa por vez. O banco precisa conter as funções da PR 31: `mottainai_analytics.fn_begin_daily_batch`, `fn_stage_operational_row`, `fn_reconcile_daily_batch`, `fn_mark_daily_batch_transformed` e `fn_publish_daily_batch`.

Validação:

```sql
SELECT * FROM mottainai_analytics.vw_etl_daily_batch_health;
SELECT * FROM mottainai_analytics.vw_published_batch;
```

Uma data com status `SUCCEEDED` não é carregada novamente. A procedure usa `ON CONFLICT` para permitir reprocessamento controlado.
