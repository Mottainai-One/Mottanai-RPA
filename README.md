# Mottainai RPA

Este repositório reúne as integrações entre os bancos físicos do Mottainai.

## Fluxos

| Fluxo | Origem | Destino | Entrada | Frequência |
| --- | --- | --- | --- | --- |
| Legado → operacional | PostgreSQL legado | `mottainai` | `python run_migration.py` | A cada 5 minutos e sob demanda |
| Operacional → analítico | `mottainai` | `mottainai_analytics` | `python run_analytics.py` | Diária, 02:00 America/Sao_Paulo |

O primeiro fluxo cifra nome, CPF, e-mail e telefone de funcionário com
AES-256-GCM, mantém somente HMACs de busca no cadastro e registra linhas
rejeitadas sem salvar seus valores. Quando o legado não fornece `firebase_uid`,
a conta é importada como convite inativo, nunca com UID inventado. O segundo
fluxo constrói dimensões e fatos para BI, sem copiar CPF, e-mail, telefone ou
senha. Ele também materializa linhas de contagem física, transferência,
reposição, doação, descarte e estados de alerta/ação; não copia observações
livres. Cada fluxo tem configuração própria no `.env.example` e pode ser
executado isoladamente.

## Organização

- `relational_migration.py` e `run_migration.py`: migração legado → operacional.
- `analytics/` e `run_analytics.py`: carga operacional → analítico.
- `dags/`: agendamento opcional no Airflow.
- `docs/`: mapeamento de dados, preparação dos bancos e operação.
- `tests/`: verificações locais que não usam os bancos reais.

## Começar

Crie um ambiente Python, instale `requirements.txt` e copie `.env.example` para `.env`. Preencha as credenciais dos bancos usados; `.env` não deve ser commitado. Instale o schema de destino pelos scripts do repositório de banco correspondente antes de executar o fluxo. Para a carga de BI, aplique `database/analytics/15_operational_kpi_facts.sql` do repositório do banco **no MOTTAINAI_ANALITICO** antes de usar esta versão da RPA; a carga interrompe com uma mensagem específica se os fatos estiverem ausentes. A migração do legado inicia em modo de simulação; defina `MIGRATION_DRY_RUN=false` somente quando quiser gravar no operacional. Consulte `docs/OPERATIONAL_DATABASE_RUNBOOK.md` para preparar esse banco.

As chaves `EMPLOYEE_PII_AES_KEY_B64` e `EMPLOYEE_PII_LOOKUP_KEY_B64` devem
conter 32 bytes aleatórios codificados em Base64 e permanecer no secret
manager. `EMPLOYEE_PII_KEY_VERSION` identifica a versão ativa para rotação.

Execute `python -m pytest -q` para verificar o código local. As duas cargas reais exigem acesso aos bancos físicos e devem ser validadas pelas contagens e logs após a execução.
