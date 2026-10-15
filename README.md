# Mottainai RPA

Este repositório reúne as integrações entre os bancos físicos do Mottainai.

## Fluxos

| Fluxo | Origem | Destino | Entrada | Frequência |
| --- | --- | --- | --- | --- |
| Legado → operacional | PostgreSQL legado | `mottainai` | `python run_migration.py` | A cada 30 minutos |
| Operacional → analítico | `mottainai` | `mottainai_analytics` | `python run_analytics.py` | Diária, 02:00 America/Sao_Paulo |

O primeiro fluxo valida o contrato estrutural do legado antes de escrever, preserva os dados mascarados, protege dados pessoais e registra linhas rejeitadas sem salvar seus valores. O segundo constrói dimensões e fatos para BI, sem copiar CPF, e-mail, telefone ou senha. Cada fluxo tem configuração própria no `.env.example` e pode ser executado isoladamente.

O agendamento legado → operacional também está definido em `.github/workflows/legacy-to-operational.yml`. Ele começa a executar a cada 30 minutos somente depois que a PR estiver na branch padrão e os secrets do repositório estiverem configurados. A execução manual começa em modo dry-run por segurança.

## Organização

- `relational_migration.py` e `run_migration.py`: migração legado → operacional.
- `analytics/` e `run_analytics.py`: carga operacional → analítico.
- `dags/`: agendamento opcional no Airflow.
- `docs/`: mapeamento de dados, preparação dos bancos e operação.
- `tests/`: verificações locais que não usam os bancos reais.

## Começar

Crie um ambiente Python, instale `requirements.txt` e copie `.env.example` para `.env`. Preencha as credenciais dos bancos usados; `.env` não deve ser commitado. Instale o schema de destino pelos scripts do repositório de banco correspondente antes de executar o fluxo. A migração do legado inicia em modo de simulação; defina `MIGRATION_DRY_RUN=false` somente quando quiser gravar no operacional. Consulte `docs/OPERATIONAL_DATABASE_RUNBOOK.md` para preparar esse banco.

Execute `python -m pytest -q` para verificar o código local. As duas cargas reais exigem acesso aos bancos físicos e devem ser validadas pelas contagens e logs após a execução.
