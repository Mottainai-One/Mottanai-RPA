# Mottanai RPA

Automação de dados do ecossistema Mottainai. O repositório sincroniza dados de um PostgreSQL legado para o banco operacional e publica uma fotografia diária do operacional no banco analítico usado pelo BI.

## Fluxos

| Fluxo | Origem | Destino | Entrada | Agenda configurada |
|---|---|---|---|---|
| Legado → operacional | PostgreSQL legado | Schema `mottainai` no operacional | `python run_migration.py` | A cada 5 minutos; reconciliação completa diária às 01:01 em São Paulo; também aceita execução manual. |
| Operacional → analítico | Banco operacional | Schema `mottainai_analytics` | `python run_analytics.py` | Diariamente às 02:00 em `America/Sao_Paulo`; também aceita execução manual. |

As agendas estão nos workflows do GitHub Actions. Sua execução depende de os workflows estarem habilitados, dos segredos, das permissões e da disponibilidade dos bancos.

## Arquitetura

```text
PostgreSQL legado → sincronização RPA → PostgreSQL operacional
                                              │
                                              └→ carga diária → PostgreSQL analítico → BI
```

A sincronização legado → operacional usa checkpoints para identificar alterações e permite reconciliação completa. A carga operacional → analítico reconcilia dimensões e fatos em uma transação, gera um `batch_id` e registra uma publicação `SUCCEEDED` com o contrato `analytics_mart_v1`. Se a carga falhar, a transação é revertida.

Os fatos analíticos incluem vendas, estoque, contagem, transferências, reposição, doações, descartes e estados de alertas e ações. A carga seleciona os campos necessários e não copia observações livres nem os dados pessoais protegidos usados na sincronização de funcionários.

## Proteção de dados

A sincronização de funcionários usa AES-256-GCM para dados pessoais e HMACs de busca. Registros inválidos ou impossíveis de decifrar podem ser colocados em quarentena com tabela, identificador e motivo, sem registrar os valores pessoais. Se o legado não fornecer `firebase_uid`, a conta é importada como convite inativo.

A chave de origem `LEGACY_PII_KEY_B64` pode ser fornecida separadamente. Quando ausente, o código usa `EMPLOYEE_PII_AES_KEY_B64` para a origem; isso exige que seja a mesma chave usada pelo backend legado. `EMPLOYEE_PII_AES_KEY_B64` e `EMPLOYEE_PII_LOOKUP_KEY_B64` devem representar 32 bytes cada, codificados em Base64. `EMPLOYEE_PII_KEY_VERSION` identifica a versão ativa.

Mantenha chaves e credenciais no gerenciador de segredos do ambiente. Consulte o [runbook operacional](docs/OPERATIONAL_DATABASE_RUNBOOK.md) para os requisitos de dados, roles e preparação do banco.

## Banco analítico e BI

A carga exige que `vw_published_batch` esteja marcada com `publication_contract=analytics_mart_v1`. O repositório contém a [migração da view](analytics/migrations/14_publish_mart_view.sql); a execução manual do workflow chama `python -m analytics.migrate_publication` antes da carga. Essa migração altera o banco analítico e exige permissões apropriadas.

Antes de usar a carga dos indicadores operacionais, aplique `database/analytics/15_operational_kpi_facts.sql` do repositório do banco no `MOTTAINAI_ANALITICO`. A carga interrompe se os fatos obrigatórios estiverem ausentes. O BI lê o lote analítico concluído e publica suas camadas e dashboard separadamente.

## Tecnologias e requisitos

- Python 3.12 é a versão usada pelos workflows.
- PostgreSQL e `psycopg2` para as conexões de origem e destino.
- `python-dotenv` para configuração local e `cryptography` para proteção dos dados.
- GitHub Actions para as agendas; Airflow possui DAGs opcionais no repositório.
- `pytest` para os testes locais.

## Estrutura

```text
.
├── relational_migration.py       # sincronização legado → operacional
├── analytics/
│   ├── load.py                   # carga operacional → analítico
│   ├── migrate_publication.py
│   └── migrations/
├── run_migration.py
├── run_analytics.py
├── analytics_daily.py            # caminho analítico alternativo
├── pipeline.py                   # pipeline genérico
├── dags/
├── docs/
├── tests/
├── .env.example
└── requirements.txt
```

## Configuração

Crie um ambiente Python, instale `requirements.txt` e configure as variáveis adequadas aos bancos usados. O [.env.example](.env.example) documenta os campos de conexão do legado e operacional e as chaves necessárias à sincronização; a configuração analítica inclui as variáveis `ANALYTICS_DB_*` usadas por `config.py` e pelo workflow analítico.

Confira roles, schema, portas, SSL e chaves no ambiente autorizado. Não versione `.env` nem valores reais de segredos.

## Execução

```bash
python -m venv .venv
python -m pip install -r requirements.txt
python -m pytest -q
```

Ative o ambiente virtual conforme o sistema operacional. Para a sincronização legado → operacional, `run_migration.py` usa simulação por padrão quando `MIGRATION_DRY_RUN` não é definido. Verifique os parâmetros e as credenciais antes de permitir gravações.

```bash
python run_migration.py
python run_analytics.py
```

`run_analytics.py` é a carga analítica ativa e **grava e faz commit** no destino; ela não oferece dry-run. O caminho alternativo `run_analytics_daily.py` usa `ANALYTICS_DRY_RUN`, mas não substitui automaticamente o fluxo agendado. Evite executar agendas de GitHub Actions e Airflow sobre os mesmos bancos sem coordenar concorrência.

## Operação e validação

Os workflows estão em [.github/workflows](.github/workflows). A execução manual da sincronização oferece opções de simulação e reconciliação completa; a agendada é configurada para gravar. Após uma carga real, confira contagens, logs e o `batch_id` no analítico. As verificações em `tests/` não substituem uma validação com bancos reais e dados autorizados.

## Documentação e licença

- [Preparação do banco operacional](docs/OPERATIONAL_DATABASE_RUNBOOK.md)
- [Carga analítica diária](docs/ANALYTICS_DAILY_LOAD.md)
- [Agendamento e publicação analítica](docs/GITHUB_DAILY_LOAD.md)
- [Mapeamento do legado](docs/MAPPING.md)
- [Licença MIT](LICENSE)
