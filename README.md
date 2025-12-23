# att_preventivos

Automacao para atualizar status em planilhas do Google Sheets:
- PREVENTIVOS (por pedido)
- COBRANCA (por NF + CNPJ)

Os scripts consultam a API do Confirma Facil, classificam como `ENTREGUE`, `CANCELADO` ou `-`
quando nao ha ocorrencia relevante, e escrevem o status na coluna de saida da planilha.

## Estrutura do repo
- `attStatusPreventivos_preventivos.py`: atualiza a aba PREVENTIVOS (pedido)
- `attStatusPreventivos_cobranca.py`: atualiza a aba COBRANCA (NF + CNPJ)
- `requirements.txt`: dependencias para rodar localmente e no CI
- `.github/workflows/att_preventivos.yml`: workflow do GitHub Actions

## Dependencias
Python 3.10+ recomendado.

Instale:
```powershell
pip install -r requirements.txt
```

## Credenciais e secrets
Os scripts leem credenciais por variaveis de ambiente (quando disponiveis):

- `CF_EMAIL`: email do Confirma Facil
- `CF_SENHA`: senha do Confirma Facil
- `GOOGLE_CREDENTIALS_PATH`: caminho local do JSON da service account
- `SHEET_ID_PREVENTIVOS`
- `SHEET_RANGE_INPUT_PREVENTIVOS`
- `SHEET_RANGE_OUTPUT_PREVENTIVOS`
- `SHEET_ID_COBRANCA`
- `SHEET_RANGE_INPUT_COBRANCA`
- `SHEET_RANGE_OUTPUT_COBRANCA`

Valores padrao (hardcoded) continuam no codigo, mas no CI usamos secrets/envs.

## Executar localmente
1) Ajuste o `GOOGLE_CREDENTIALS_PATH` para o caminho do JSON local.
2) Defina variaveis de ambiente se quiser sobrescrever o que esta no codigo.
3) Execute:

```powershell
python .\attStatusPreventivos_preventivos.py
python .\attStatusPreventivos_cobranca.py
```

## GitHub Actions
Workflow: `.github/workflows/att_preventivos.yml`

Secrets necessarios:
- `CF_EMAIL`
- `CF_SENHA`
- `GOOGLE_SERVICE_ACCOUNT_JSON_B64` (conteudo do JSON em base64)

Para gerar o base64 do JSON:
```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("C:\caminho\credenciais.json"))
```

O workflow:
1) Instala dependencias
2) Gera `credentials.json` a partir do secret base64
3) Executa os dois scripts

## Observacoes importantes
- Se a planilha tiver cabecalho, o script escreve `STATUS` na linha 1 da coluna de saida.
- Linhas vazias nao recebem status.
- Se nao houver ocorrencia de entrega/cancelamento, o status fica `-`.
- Os ranges padrao estao no topo de cada script; altere se a planilha mudar.
