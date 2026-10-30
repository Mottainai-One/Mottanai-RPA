# Runbook do banco operacional — Mottainai RPA

Este documento contém as queries necessárias para preparar e validar o banco operacional antes da migração do legado.

Banco esperado: `MOTTAINAI`  
Schema esperado: `mottainai`  
Fonte: `defaultdb.public`

## 1. Confirmar banco e schema

Execute conectado ao banco `MOTTAINAI`:

```sql
SELECT current_database(), current_schema();

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'mottainai'
ORDER BY table_name;
```

As tabelas-base esperadas são `address`, `employee_role`, `subscription_plan`,
`company`, `retail_store`, `employee`, `employee_private_data` e `app_user`.

O schema final não possui `employee.name/cpf/email/phone` nem
`app_user.cpf/email`. A RPA detecta automaticamente a fase transitória, mas
sempre grava o envelope em `employee_private_data`.

## 2. Funções de proteção

```sql
CREATE OR REPLACE FUNCTION mottainai.fn_is_sha256_token(p_value TEXT, p_prefix TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT p_value ~ ('^' || p_prefix || '_SHA256_[A-Fa-f0-9]{64}$');
$$;

CREATE OR REPLACE FUNCTION mottainai.fn_is_bcrypt_hash(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT p_value ~ '^\$2[aby]\$[0-3][0-9]\$[./A-Za-z0-9]{53}$';
$$;
```

## 3. Compatibilidade com dados já mascarados do legado

```sql
CREATE OR REPLACE FUNCTION mottainai.fn_is_legacy_masked_email(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT p_value ~* '^email_emp_[0-9]+$';
$$;

CREATE OR REPLACE FUNCTION mottainai.fn_is_legacy_masked_phone(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT p_value ~* '^phone_emp_[0-9]+$';
$$;

CREATE OR REPLACE FUNCTION mottainai.fn_is_legacy_masked_cpf(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT p_value ~ '^[0-9]{4}$';
$$;

CREATE OR REPLACE FUNCTION mottainai.fn_is_protected_email(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT mottainai.fn_is_sha256_token(p_value, 'EMAIL')
        OR mottainai.fn_is_legacy_masked_email(p_value)
        OR p_value ~ '^EMAIL_[A-Z]+_[0-9]+$'
        OR p_value ~ '^USER_[0-9]+$'
        OR p_value ~ '^[A-Za-z0-9._%+-]*\*{3}@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';
$$;

CREATE OR REPLACE FUNCTION mottainai.fn_is_protected_phone(p_value TEXT)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT mottainai.fn_is_sha256_token(p_value, 'PHONE')
        OR mottainai.fn_is_legacy_masked_phone(p_value)
        OR p_value ~ '^PHONE_[A-Z]+_[0-9]+$'
        OR p_value ~ '^\([0-9]{2}\) 9\*{4}-[0-9]{4}$';
$$;
```

## 4. Auditoria e contexto de importação

A PR deve incorporar a alteração equivalente na função `fn_trg_audit_log`: durante `legacy-import`, o `user_id` da auditoria deve ser `NULL`, nunca `0`, e a exigência de contexto validado deve ser ignorada somente nesse contexto controlado.

```sql
CREATE OR REPLACE FUNCTION mottainai.fn_set_legacy_migration_context(p_company_id INTEGER)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = mottainai, pg_temp
AS $$
BEGIN
    IF p_company_id IS NULL OR p_company_id <= 0 THEN
        RAISE EXCEPTION 'company_id inválido para migração';
    END IF;

    PERFORM set_config('app.migration_context', 'legacy-import', FALSE);
    PERFORM set_config('app.current_company_id', p_company_id::TEXT, FALSE);
END;
$$;
```

Na função de auditoria:

```sql
v_migration_context := current_setting('app.migration_context', TRUE);

IF v_migration_context = 'legacy-import' THEN
    v_user_id := NULL;
END IF;
```

E a condição de bloqueio deve excluir o contexto de importação:

```sql
AND v_migration_context IS DISTINCT FROM 'legacy-import'
AND (v_user_id IS NULL OR v_company_id IS NULL)
```

Não remover triggers, RLS ou constraints.

## 5. Verificações antes do DRY_RUN

```sql
SELECT
    to_regprocedure('mottainai.fn_is_sha256_token(text,text)') AS sha256_token,
    to_regprocedure('mottainai.fn_is_bcrypt_hash(text)') AS bcrypt_hash,
    to_regprocedure('mottainai.fn_is_legacy_masked_email(text)') AS legacy_email,
    to_regprocedure('mottainai.fn_is_legacy_masked_phone(text)') AS legacy_phone,
    to_regprocedure('mottainai.fn_is_legacy_masked_cpf(text)') AS legacy_cpf;

SELECT
    mottainai.fn_is_protected_email('email_emp_001') AS legacy_email_ok,
    mottainai.fn_is_protected_phone('PHONE_EMP_001') AS legacy_phone_ok,
    mottainai.fn_is_legacy_masked_cpf('9040') AS legacy_cpf_ok,
    mottainai.fn_is_sha256_token('EMAIL_SHA256_' || repeat('A', 64), 'EMAIL') AS sha_ok;
```

Os resultados esperados são `true` para todos os campos de teste.

## 6. Verificar dados após uma migração aprovada

Estas queries devem ser executadas somente depois que o usuário rodar a carga real:

```sql
SELECT COUNT(*) FROM mottainai.address;
SELECT COUNT(*) FROM mottainai.employee_role;
SELECT COUNT(*) FROM mottainai.subscription_plan;
SELECT COUNT(*) FROM mottainai.company;
SELECT COUNT(*) FROM mottainai.retail_store;
SELECT COUNT(*) FROM mottainai.employee;
SELECT COUNT(*) FROM mottainai.app_user;
```

Verificação de relacionamentos:

```sql
SELECT COUNT(*) AS employees_without_store
FROM mottainai.employee e
LEFT JOIN mottainai.retail_store s ON s.store_id = e.store_id
WHERE s.store_id IS NULL;

SELECT COUNT(*) AS stores_without_company
FROM mottainai.retail_store s
LEFT JOIN mottainai.company c ON c.company_id = s.company_id
WHERE c.company_id IS NULL;

SELECT COUNT(*) AS users_without_employee
FROM mottainai.app_user u
LEFT JOIN mottainai.employee e ON e.employee_id = u.employee_id
WHERE e.employee_id IS NULL;
```

Todos os contadores de órfãos devem ser `0`.

## 7. Execução da RPA

Antes do `DRY_RUN`, configure no secret manager:

- `EMPLOYEE_PII_AES_KEY_B64`: chave AES de 32 bytes em Base64;
- `EMPLOYEE_PII_LOOKUP_KEY_B64`: chave HMAC distinta, também com 32 bytes;
- `EMPLOYEE_PII_KEY_VERSION`: inteiro positivo da versão ativa.

O login operacional deve ser membro apenas de `mottainai_migrator`. A carga
assume essa role com `SET LOCAL ROLE`, estabelece o contexto da empresa e
mantém RLS e auditoria ativos.

Teste seguro, sem persistir alterações:

```powershell
$env:MIGRATION_DRY_RUN="true"
.\.venv\Scripts\python.exe -u run_migration.py
```

Carga definitiva — executada somente pelo responsável:

```powershell
$env:MIGRATION_DRY_RUN="false"
.\.venv\Scripts\python.exe -u run_migration.py
```

A migração relacional lê integralmente as tabelas de origem. Depois de limpar
um dataload de teste no operacional, execute manualmente com `force_full=true`
para recompor a carga sem alterar o banco legado. O agendamento normal consulta
o legado a cada 5 minutos e evita regravar
linhas com o mesmo HMAC de conteúdo em `legacy_sync_record`. Uma reconciliação
completa roda às 01:01 de São Paulo para corrigir desvios. Uma carga completa
levou cerca de 5m27s no teste de 30/10/2026; o GitHub Actions pode atrasar ou
enfileirar execuções, portanto 5 minutos é a frequência solicitada, não uma
garantia de latência. CNPJs inválidos são colocados em quarentena pelo ID de
origem, sem alterar o identificador nem afrouxar as regras do operacional.
