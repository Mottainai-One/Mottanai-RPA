# Fluxo alternativo de staging do banco analítico

> **Não é o workflow de produção atualmente agendado.** O GitHub Actions ativo
> é `.github/workflows/operational-to-analytics.yml`, que executa
> `run_analytics.py` e publica os fatos diretamente, registrando a publicação
> em `job_log` e `vw_published_batch`. Ele não grava em `etl_daily_batch`.
> Este script alternativo grava o staging e o estado do batch, mas ainda não
> transforma as linhas staged nas dimensões e fatos consumidos pelo dashboard.
> Não agende este fluxo nem o use como substituto do workflow ativo até a
> implementação e validação dessa transformação.

O script experimental `run_analytics_daily.py` lê o banco operacional `MOTTAINAI` e cria o estado de batch/staging no schema `mottainai_analytics`. A data de referência é o dia anterior no fuso `America/Sao_Paulo`. Ele não acessa o legado.

O script usa duas conexões: `OPERATIONAL_DB_*` para leitura e `ANALYTICS_DB_*` para staging. O banco analítico deve conter o schema `mottainai_analytics` e a tabela `etl_source_contract` instalada pela PR 35 do banco.

O contrato precisa refletir o schema operacional: `customer_consent` usa `recorded_at` e `loyalty_redemption` usa `redeemed_at`. Antes de abrir o batch, a RPA valida todas as tabelas e colunas incrementais contra o banco operacional.

Na PR 34, os campos de identidade de `employee` e `app_user` podem ser removidos após a finalização AES-256-GCM. A RPA analítica nunca copia esses campos, inclusive `cpf_ciphertext`, `cpf_nonce`, `cpf_masked`, `email_ciphertext`, `email_nonce`, `email_masked`, `phone_ciphertext`, `phone_nonce`, `phone_masked`, `key_version` e `encrypted_at`.

Inspeção local somente para o fluxo de staging:

```powershell
$env:ANALYTICS_DRY_RUN="true"
.\.venv\Scripts\python.exe -u run_analytics_daily.py
```

O modo `ANALYTICS_DRY_RUN=true` reverte a transação. **Não altere para `false`
em produção:** o código atual conclui o estado de batch/staging, mas ainda não
materializa os fatos e dimensões do data mart.

O DAG `dags/mottainai_analytics_daily.py` fica sem agenda automática (`schedule=None`) para evitar criar batches que não materializam o data mart. Ele mantém três retries e uma execução ativa por vez se for acionado manualmente durante desenvolvimento. O banco precisa conter as funções da PR 35: `mottainai_analytics.fn_begin_daily_batch`, `fn_stage_operational_row`, `fn_reconcile_daily_batch`, `fn_mark_daily_batch_transformed` e `fn_publish_daily_batch`.

Validação do fluxo de staging (não valida a publicação dos fatos do dashboard):

```sql
SELECT * FROM mottainai_analytics.vw_etl_daily_batch_health;
SELECT batch_id, status, business_date, scheduled_at, cutoff_at,
       staged_rows, rejected_rows, rows_written, error_message
FROM mottainai_analytics.etl_daily_batch
ORDER BY started_at DESC NULLS LAST
LIMIT 5;
```

Uma data com status `PUBLISHED` não é carregada novamente. A procedure usa o identificador do batch e checkpoints para permitir reprocessamento controlado. O status `PUBLISHED` deste protótipo significa apenas que o estágio foi aceito; não prova que os fatos do dashboard foram atualizados.

Registros históricos deste fluxo alternativo não devem ser usados como evidência de execução do workflow ativo. Para validar o carregador agendado, use as consultas de `docs/GITHUB_DAILY_LOAD.md`.
