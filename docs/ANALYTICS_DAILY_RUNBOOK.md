# Carga diaria do banco analitico

O job `mottainai_operational_to_analytics_daily` lê o banco operacional `MOTTAINAI` e publica um batch no schema `mottainai_analytics` diariamente às 02:00. A data de referência é o dia anterior no fuso `America/Sao_Paulo`. Ele não acessa o legado.

O job usa duas conexões: `OPERATIONAL_DB_*` para leitura e `ANALYTICS_DB_*` para staging/publicação. O banco analítico deve conter o schema `mottainai_analytics` e a tabela `etl_source_contract` instalada pela PR 35 do banco.

O contrato precisa refletir o schema operacional: `customer_consent` usa `recorded_at` e `loyalty_redemption` usa `redeemed_at`. Antes de abrir o batch, a RPA valida todas as tabelas e colunas incrementais contra o banco operacional.

Na PR 34, os campos de identidade de `employee` e `app_user` podem ser removidos após a finalização AES-256-GCM. A RPA analítica nunca copia esses campos, inclusive `cpf_ciphertext`, `cpf_nonce`, `cpf_masked`, `email_ciphertext`, `email_nonce`, `email_masked`, `phone_ciphertext`, `phone_nonce`, `phone_masked`, `key_version` e `encrypted_at`.

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

O DAG `dags/mottainai_analytics_daily.py` usa `0 2 * * *`, três retries, intervalo de dez minutos e uma execução ativa por vez. O banco precisa conter as funções da PR 35: `mottainai_analytics.fn_begin_daily_batch`, `fn_stage_operational_row`, `fn_reconcile_daily_batch`, `fn_mark_daily_batch_transformed` e `fn_publish_daily_batch`.

Validação:

```sql
SELECT * FROM mottainai_analytics.vw_etl_daily_batch_health;
SELECT * FROM mottainai_analytics.vw_published_batch;
```

Uma data com status `PUBLISHED` não é carregada novamente. A procedure usa o identificador do batch e checkpoints para permitir reprocessamento controlado.

Execução confirmada em 2026-10-19: batch publicado para `2026-10-18`, com `rows_staged = 0` e `rows_written = 0`. Isso significa que o pipeline funcionou e não havia registros novos no período.
