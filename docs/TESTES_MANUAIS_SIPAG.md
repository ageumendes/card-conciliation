# Testes Manuais Sipag

## 1. Introducao

Objetivo dos testes
- Validar a integracao com a API da Sipag ponta a ponta.
- Garantir autenticacao, importacao D-1, persistencia e normalizacao.
- Servir como checklist operacional para suporte e operacao.

Quando executar
- Primeira integracao com a Sipag.
- Troca/renovacao de token.
- Erros de conciliacao ou divergencias operacionais.

Pre-requisitos
- API rodando localmente (Nest application successfully started).
- Firebird ativo e acessivel.
- Tabelas criadas (ACQ_RAW, ACQ_TX, JOB_RUNS, etc.).

## 2. Variaveis e dependencias

Variaveis .env usadas pela Sipag
- SIPAG_BASE_URL
- SIPAG_ENDPOINT_EXTRATO
- SIPAG_TOKEN

Dependencias
- Porta padrao da API: 3000
- Endpoint manual de importacao: `POST /admin/sipag/import?date=YYYY-MM-DD`

## 3. Teste 1 — Health check da API

Objetivo: garantir que o backend esta rodando.

Comando:
```
curl http://localhost:3000
```

Resultado esperado:
- API responde.
- Nenhum erro no log.

## 4. Teste 2 — Autenticacao Sipag

Objetivo: validar token / headers.

Executar:
```
curl -X POST "http://localhost:3000/admin/sipag/import?date=YYYY-MM-DD"
```

Resultados esperados:
- 401 / 403 -> erro de credencial.
- 200 -> autenticacao valida.

## 5. Teste 3 — Importacao D-1 valida

Objetivo: importar transacoes do dia anterior.

Data de exemplo:
- YYYY-MM-DD = ontem

Executar:
```
curl -X POST "http://localhost:3000/admin/sipag/import?date=YYYY-MM-DD"
```

Resultado esperado:
```
{
  "ok": true,
  "imported": N,
  "rawId": X
}
```

## 6. Teste 4 — Persistencia no Firebird

Objetivo: confirmar gravacao correta.

Rodar SQLs:
```
SELECT * FROM ACQ_RAW WHERE ACQUIRER='SIPAG' AND REF_DATE='YYYY-MM-DD';
SELECT * FROM ACQ_TX  WHERE ACQUIRER='SIPAG' AND REF_DATE='YYYY-MM-DD';
```

Observacao: se suas tabelas usam a coluna `PROVIDER`, ajuste os SQLs para `PROVIDER='SIPAG'`.

Resultados esperados:
- 1 registro em ACQ_RAW.
- N registros em ACQ_TX.

## 7. Teste 5 — Idempotencia (reimportacao)

Objetivo: evitar duplicidade.

Executar novamente o mesmo import:
```
curl -X POST "http://localhost:3000/admin/sipag/import?date=YYYY-MM-DD"
```

Resultado esperado:
- Nao duplicar registros.
- Log informando import ja existente ou reprocessado.

## 8. Teste 6 — Datas invalidas

Testar:
- Data futura.
- Data vazia.
- Data mal formatada.

Exemplos:
```
curl -X POST "http://localhost:3000/admin/sipag/import?date=2099-01-01"
curl -X POST "http://localhost:3000/admin/sipag/import?date="
curl -X POST "http://localhost:3000/admin/sipag/import?date=2024/01/01"
```

Resultado esperado:
- Erro tratado.
- Mensagem clara no response.

## 9. Teste 7 — Falhas de rede / timeout

Simular:
- Base URL invalida (ex: SIPAG_BASE_URL=http://localhost:9999).
- API fora do ar.

Resultado esperado:
- Job nao quebra a aplicacao.
- Erro registrado em JOB_RUNS.

## 10. Logs esperados

Exemplo de log de sucesso:
```
[LOG] Job D-1 iniciado: 2024-05-20
[LOG] Sipag D-1 concluido: 120 transacoes
[LOG] Job D-1 finalizado com sucesso: 2024-05-20
```

Exemplo de log de erro:
```
[ERROR] Job D-1 falhou: Request failed with status code 401
```

Exemplo de log de excecao Sipag:
```
[ERROR] Sipag request error: ECONNABORTED timeout of 10000ms exceeded
```

## 11. Checklist final

- [ ] API sobe
- [ ] Token valido
- [ ] Importa D-1
- [ ] Salva RAW
- [ ] Normaliza
- [ ] Grava ACQ_TX
- [ ] Nao duplica
- [ ] Loga JOB_RUNS
