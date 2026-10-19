# Integração da RPA legada e carga analítica diária

## O que foi entregue

- Integração do banco legado com o banco operacional `MOTTAINAI`.
- Carga diária do operacional para o banco analítico às 02:00.
- Conexões independentes para operacional e analítico.
- Proteção de dados sensíveis antes da publicação analítica.
- Execução idempotente por batch.
- Validação do contrato analítico contra as tabelas reais do banco operacional antes de abrir um batch.
- Documentação dos comandos SQL e das conexões dos dois bancos.

## Segurança e operação

- Nenhuma constraint de segurança é desativada.
- `.env` e credenciais não são versionados.
- O `dry run` faz rollback completo.
- Arquivos de testes não fazem parte desta PR.

## Verificação manual

```powershell
$env:ANALYTICS_DRY_RUN="true"
.\.venv\Scripts\python.exe -u run_analytics_daily.py
```

Antes da execução, o contrato `mottainai_analytics.etl_source_contract` deve estar instalado no banco analítico e todos os `watermark_column` incrementais devem existir no schema operacional `mottainai`.
