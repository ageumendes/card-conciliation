# Frontend - Card Conciliation

Dashboard de conciliacao para consumo dos endpoints `/admin/*` do backend NestJS.

## Requisitos
- Node.js 18+
- Backend rodando em http://localhost:3000

## Configuracao
Crie `.env` com base em `.env.example`:
```
VITE_API_BASE_URL=http://localhost:3000
```

Ao abrir o app, o usuario deve iniciar sessao com usuario e senha.
Se ainda nao existir nenhum usuario no Firebird, a interface oferece o cadastro do primeiro administrador.

## Como rodar
```
cd frontend
npm install
npm run dev
```

## Build
```
npm run build
```

## Rotas
- `/` Dashboard
- `/reconciliation` Tela principal (Modelo 1)
- `/interdata` Importacao Interdata
- `/cielo-edi` Monitoramento Cielo EDI

## Integracoes atuais
- `GET /admin/interdata/ping`
- `GET /admin/interdata/files`
- `POST /admin/interdata/import/scan`
- `POST /admin/interdata/import/upload`
- `GET /admin/interdata/sales`
- `GET /admin/cielo-edi/ping`
- `GET /admin/cielo-edi/files`
- `POST /admin/cielo-edi/scan`
- `POST /admin/cielo-edi/sync`

## Observacoes
- O Vite faz proxy de `/admin` para `VITE_API_BASE_URL` no modo dev.
- A tela de conciliacao usa vendas do INTERDATA como fonte inicial. TODO: integrar conciliacao com adquirentes.
