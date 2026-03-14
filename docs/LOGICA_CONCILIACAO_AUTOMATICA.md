# Logica de Conciliacao Automatica

Este documento descreve a logica ideal (e evolutiva) para conciliacao automatica, usando o que ja existe no backend como base.

## Objetivo
- Conciliar vendas Interdata com vendas das adquirentes (Cielo/Sipag) com alta precisao, previsibilidade e rastreabilidade.

## Pre-condicoes e normalizacao
- Normalizar identificadores: remover espacos, zeros a esquerda quando relevante.
- Normalizar datas para comparacao (timezone consistente).
- Normalizar valores monetarios para numero com 2 casas.
- Normalizar bandeira e tipo de pagamento (credito/debito).

## Matching em cascata (prioridade)
1) Match exato por NSU + valor + janela de data/hora
   - Regra: NSU (PDV ou adquirente) coincide e valor bruto difere <= X centavos.
   - Janela: tolerancia de tempo de 5 horas (+- 5h).
2) Match por AUTH_CODE + valor + data/hora
   - Regra: AUTH_CODE coincide e valor bruto difere <= X.
3) Match por TID/SALE_CODE/SALE_NO + valor
   - Regra: TID ou SALE_CODE ou SALE_NO coincide e valor bruto difere <= X.
4) Fallback por valor + janela de tempo + heuristicas
   - Regra: mesma data, valor proximo, bandeira compativel, tipo de pagamento compativel.
5) Unique-by-amount (Sipag)
   - Regra: dentro da janela, se existir exatamente 1 candidato Sipag com o mesmo valor (2 casas), concilia direto.

## Pontuacao (score) sugerida
- Base (valor): +50 se diff <= 0.02
- Mesma data: +20
- Delta horario <= 120s: +10
- Tipo pagamento compativel: +10
- Bandeira compativel: +5
- Identificador forte (NSU/AUTH/TID): +30 adicional

Sugestao de thresholds:
- Aceitar auto: score >= 80
- Review manual: score entre 70 e 79
- Rejeitar: score < 70

## Parametros fixos atuais
- Janela de tolerancia de tempo: 5 horas.
- Tolerancia de valor: 0.02 (2 centavos) para evitar falhas de arredondamento sem abrir demais a busca.

## Criterios de empate (tie-break)
- Menor diferenca de horario.
- Menor diferenca de valor.
- Maior score.
- Persistir em ordem estavel por ID (desc) para previsibilidade visual.
- Se empate forte (diferencas muito proximas), nao conciliar automaticamente.

## Criterios de bloqueio
- Divergencia de valor acima de X centavos (ex: 0.05) bloqueia conciliacao.
- Multiplos candidatos com score igual dentro do threshold bloqueiam conciliacao.
- Status cancelado/estornado na adquirente bloqueia conciliacao.

## Parametros configuraveis
- Janela de tempo por adquirente (ex: Cielo 10 min, Sipag 20 min).
- Threshold de valor (centavos).
- Threshold de score.
- Lista de status bloqueados.
- Regras por bandeira ou modalidade (ex: PIX separado).

## Saidas esperadas e codigos de motivo
Campos recomendados no registro de conciliacao:
- `MATCH_TYPE` (ja existe)
- `MATCH_SCORE` (ja existe)
- `AMOUNT_DIFF` (ja existe)
- `SOURCE` (AUTO/MANUAL)
- `AUTO_REASON_CODE` (novo, opcional)
- `AUTO_REASON_TEXT` (novo, opcional)

Codigos sugeridos:
- `AUTO_NSU_EXACT`
- `AUTO_AUTH_EXACT`
- `AUTO_TID_EXACT`
- `AUTO_SALE_CODE_EXACT`
- `AUTO_TIME_AMOUNT`
- `AUTO_FALLBACK_HEURISTIC`
- `AUTO_BLOCKED_AMOUNT_DIFF`
- `AUTO_BLOCKED_MULTI_MATCH`

## Como validar e testar
- Montar datasets com casos:
  - Match perfeito (NSU + valor + data).
  - Mesmo valor com horarios distantes (deve bloquear).
  - Multiplos candidatos com mesmo score (deve bloquear).
  - Casos de cancelamento/estorno.
- Comparar resultados com conciliacao manual (golden set).
- Medir taxa de acerto por adquirente e ajuste de janela/score.
