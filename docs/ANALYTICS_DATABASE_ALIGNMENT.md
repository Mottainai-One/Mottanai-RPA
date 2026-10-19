# Alinhamento dos bancos operacional e analítico

A RPA lê o banco operacional `MOTTAINAI` no schema `mottainai` e publica no banco analítico no schema `mottainai_analytics`. O contrato de extração fica no banco analítico; as colunas usadas pelo contrato precisam existir no banco operacional.

## 1. Verificar o banco operacional

Execute conectado ao banco operacional `MOTTAINAI`:

```sql
SELECT current_database(), current_schema();

SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'mottainai'
ORDER BY table_name, ordinal_position;
```

## 2. Verificar o contrato analítico

Execute conectado ao banco analítico:

```sql
SELECT current_database(), current_schema();

SELECT source_table, extraction_mode, watermark_column, required
FROM mottainai_analytics.etl_source_contract
WHERE enabled
ORDER BY load_order;
```

## 3. Encontrar divergências automaticamente

Como os bancos são independentes, execute esta consulta no banco analítico. Ela consulta o catálogo do banco analítico apenas para validar que o contrato está instalado; a validação final das tabelas operacionais é feita pela RPA usando a conexão `OPERATIONAL_DB_*`.

```sql
SELECT to_regclass('mottainai_analytics.etl_source_contract') AS contract_table;
```

O resultado deve ser `mottainai_analytics.etl_source_contract`.

## 4. Regras do contrato

- `INCREMENTAL` exige que `watermark_column` exista na tabela correspondente do schema `mottainai`.
- `SNAPSHOT` deve ter `watermark_column` nulo.
- `inventory` e `customer_consent` podem permanecer como `SNAPSHOT` quando não houver uma coluna de alteração confiável.
- Não crie colunas artificiais apenas para satisfazer a RPA. Se uma tabela não possuir um timestamp confiável, altere o contrato para `SNAPSHOT` ou inclua uma coluna de auditoria na PR do banco operacional.

## 5. Variáveis de conexão

```env
OPERATIONAL_DB_HOST=
OPERATIONAL_DB_PORT=5432
OPERATIONAL_DB_NAME=MOTTAINAI
OPERATIONAL_DB_USER=
OPERATIONAL_DB_PASSWORD=
OPERATIONAL_DB_SSLMODE=require

ANALYTICS_DB_HOST=
ANALYTICS_DB_PORT=5432
ANALYTICS_DB_NAME=
ANALYTICS_DB_USER=
ANALYTICS_DB_PASSWORD=
ANALYTICS_DB_SSLMODE=require
```

Nunca publique `.env` ou senhas no repositório. Depois de instalar o contrato no banco analítico e alinhar as colunas no operacional, execute primeiro em modo seguro:

```powershell
$env:ANALYTICS_DRY_RUN="true"
.\.venv\Scripts\python.exe -u run_analytics_daily.py
```

O RPA agora valida o contrato antes de abrir o batch. Se houver divergência, nenhuma publicação é iniciada e a mensagem lista as tabelas/colunas incompatíveis.
