# Carga diaria do banco analitico

O job `mottainai_operational_to_analytics_daily` lê o banco operacional `MOTTAINAI` e atualiza o schema `analytics` diariamente às 02:00. A data de referência é o dia anterior no fuso `America/Sao_Paulo`. Ele não acessa o legado.

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

O DAG `dags/mottainai_analytics_daily.py` usa `0 2 * * *`, três retries, intervalo de dez minutos e uma execução ativa por vez. O banco precisa conter as procedures `analytics.refresh_dimensions()` e `analytics.refresh_facts(date)` do script do banco analítico.

Validação:

```sql
SELECT * FROM analytics.etl_run_log ORDER BY started_at DESC;
SELECT * FROM analytics.fact_store_daily
WHERE date_key=to_char(current_date-1,'YYYYMMDD')::integer;
```

Uma data com status `SUCCEEDED` não é carregada novamente. A procedure usa `ON CONFLICT` para permitir reprocessamento controlado.
