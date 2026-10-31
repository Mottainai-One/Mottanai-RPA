# Contrato de migração

## Fontes identificadas no diagrama legado

O diagrama fornecido apresenta as entidades `Subscription_plan`, `Address`, `Company`, `EmployeeRole`, `Store`, `Employee`, `UserAccount` e `Administration`. O diagrama é tratado como referência de negócio; os nomes e tipos definitivos devem ser confirmados por introspecção no banco de origem.

Premissas confirmadas para a primeira implementação: schema de origem `public`; cada empresa pode possuir várias lojas; cada loja possui um endereço; funcionários e suas contas de usuário também devem ser migrados.

## Destino confirmado

O banco operacional define, entre outras, as tabelas:

| Ordem | Tabela | Dependências |
|---:|---|---|
| 1 | `subscription_plan` | — |
| 2 | `address` | — |
| 3 | `company` | `subscription_plan` |
| 4 | `employee_role` | — |
| 5 | `retail_store` | `company`, `address` |
| 6 | `employee` | `retail_store`, `employee_role`; mantém somente HMACs de CPF/e-mail para busca |
| 7 | `employee_private_data` | `employee`; envelopes AES-256-GCM de nome, CPF, e-mail e telefone |
| 8 | `app_user` | `employee`; não duplica CPF/e-mail |

Depois dessas tabelas-base, a carga pode avançar para catálogo, compras/recebimento, estoque, vendas, transferências, doações, descarte, reposição, alertas e inventário.

## Regras de transformação

- IDs do legado devem ser preservados somente se não colidirem; caso contrário, manter uma tabela de correspondência `source_system/source_id/target_id`.
- CPF, CNPJ e CEP devem ser normalizados para apenas dígitos.
- UF deve ser maiúscula com dois caracteres.
- E-mails devem ser normalizados sem espaços e validados antes da carga.
- Textos devem passar por Unicode NFKC, remoção de NUL e compactação de espaços.
- Datas inválidas ou campos obrigatórios ausentes vão para quarentena.
- `deleted_at` deve ser preservado para respeitar soft delete.
- Senhas nunca devem ser migradas em texto puro; `password_hash` só pode receber hashes compatíveis e previamente autorizados.
- nome, CPF, e-mail e telefone do funcionário são cifrados separadamente com
  AES-256-GCM, nonce aleatório de 12 bytes e AAD
  `mottainai:employee:<employee_id>:<campo>:v1`;
- os HMAC-SHA-256 de CPF e e-mail usam chave externa e separação de domínio;
- a recuperação possível no operacional é exatamente a do valor recebido da
  origem: dado já mascarado ou irreversível no legado não volta a ser aberto;
- `app_user` não recebe CPF/e-mail; sem `firebase_uid`, a conta entra inativa
  até o backend concluir o convite no Firebase.

## Pontos ainda necessários

O diagrama não informa de forma inequívoca:

1. nomes físicos/schema das tabelas legadas;
2. coluna de alteração incremental;
3. relação exata entre empresa e plano;
4. relação de loja com endereço;
5. formato do hash de senha legado;
6. estratégia para registros sem correspondência de FK.

Por isso, a execução produtiva deve começar com `DRY_RUN=true` e com a inspeção de schema já existente em `tools/inspect_legacy_schema.py`. A carga genérica atual não inventa valores para FKs obrigatórias.

## Usuários e senhas

As contas devem ser migradas depois de `employee_private_data`, preservando o
vínculo `app_user.employee_id`. Se o legado já guardar um hash compatível, o
hash pode ser transportado sem conhecer a senha. Se guardar senha em texto
puro, ela não deve ser copiada: o processo deve forçar redefinição no primeiro
acesso. A RPA nunca registra senha, chave, plaintext ou envelope em logs,
auditoria ou quarentena.
