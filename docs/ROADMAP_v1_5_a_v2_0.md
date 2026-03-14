# Roadmap v1.5 -> v2.0

## v1.5 (rapido, 1-2 dias)
- Filtros 100% funcionais e sincronizados com URL.
- Ordenacao por data/valor consistente (backend como fonte unica).
- Header da tabela com resumo compacto.
- Pequenos ajustes de consistencia e limpeza de estados.
- QA basico no fluxo manual.

## v1.6 (1 semana)
- Conciliacao semi-automatica com score e tela de sugestoes.
- Workflow "aprovar sugestoes" com batch select.
- Registro de motivo/score em conciliacoes automaticas.
- Ajustes de performance em queries com filtros.

## v1.7
- Auditoria completa (quem conciliou, regra aplicada, motivo).
- Logs estruturados e relatorios de divergencia.
- Exportacao CSV/Excel com filtros e totals.
- Alertas para divergencias recorrentes.

## v2.0
- Motor de conciliacao configuravel (regras por adquirente).
- Parametros por cliente (janela, thresholds, penalidades).
- Observabilidade (metrics, tracing, dashboards).
- Permissoes/roles (admin, operador, auditor).
- Otimizacao de performance (indices, filas, jobs escalaveis).
