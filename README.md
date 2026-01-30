# WebhookFollow

WebhookFollow agora roda em lote: o runner coleta snapshots GreenMile para todas as rotas monitoradas no ClickUp, normaliza cada parada (ordenada e sem campos voláteis), calcula um fingerprint SHA-256, confere mudanças via `ScriptProperties` e publica um único webhook por execução quando algo mudou. ClickUp é atualizado somente para as rotas que realmente mudaram e o site recebe um payload delta ou uma notificação pull.

## Configuração obrigatória
| Propriedade | Descrição |
| --- | --- |
| `LOOP_CLICKUP_LIST_ID` | Lista do ClickUp cuja tasks ativas representam planos monitorados. |
| `CLICKUP_TOKEN` | Token usado por `cuFetch_`. |
| `GM_USERNAME` / `GM_PASSWORD` | Credenciais GreenMile utilizadas para buscar o token (cacheado em `WF_GM_TOKEN`). |
| `SITE_WEBHOOK_URL` | URL para onde o runner publica o update único. |
| `SITE_WEBHOOK_SECRET` (opcional) | Segredo HMAC SHA-256 que gera o cabeçalho `X-WF-Signature`. |
| `SITE_WEBHOOK_MODE` | `delta` (default) envia `changedRouteIds`; `pull` envia somente `dataVer` e o site chama `getSnapshot`. |
| `SITE_WEBHOOK_HEARTBEAT` | `true` habilita webhook leve quando nada mudou. Default `false`. |
| `MODE` | `BATCH_WEBHOOK` (default) usa o pipeline novo; `LEGACY` mantém o laço clássico que chamava `sincronizarGreenMileStable` para cada task. |
| `REFRESH_GM_FIELD_ID` | ID do campo personalizado checkbox `💹 Atualizar GM`; o loop marca esse campo para disparar o webhook do ClickUp e o doPost limpa o checkbox ao terminar. |

## Execução em lote
- O runner respeita `LockService`, evita concorrência e faz reschedule (45s ou 90s após erro).
- `fetchGreenMileSnapshots` usa `gmFetchBatch_` com `UrlFetchApp.fetchAll` e chunks de 50 rotas para reduzir chamadas.
- Cada rota recebe objetos flatten (como `stop.plannedSequenceNum`, `stop.deliveryStatus`, `stop.location.key`) sem campos `clickup.*` e sem timestamps voláteis (`lastUpdatedAt`, `now`, `timestamp`, `createdAt`).
- Fingerprints são persistidos em `WF_ROUTE_FP:<rota>` somente após `sincronizarGreenMileStable` concluir com sucesso.
- Ao detectar mudanças, o runner incrementa `WF_BATCH_DATA_VERSION` e registra o summary em `WF_LAST_RUN_SUMMARY`.
- O `executarLoopListaClickUp` atualiza apenas o checkbox `💹 Atualizar GM` (configurado por `REFRESH_GM_FIELD_ID`); o ClickUp dispara o webhook e é o `doPost` o responsável por baixar o GreenMile, aplicar mudanças, comentar e avisar o site.

## Modos do webhook
### Payload delta (default)
```json
{
  "source": "WebhookFollow",
  "execId": "wf-1700000000000-abc123",
  "dataVer": "42",
  "ts": 1700000000000,
  "changedRouteIds": ["6100001", "6100002"],
  "counters": { "checked": 54, "changed": 2, "errors": 0 },
  "mode": "delta"
}
```
O site deve re-renderizar apenas as rotas listadas ou chamar `getSnapshot` caso precise dos dados completos.

### Payload pull-only (`SITE_WEBHOOK_MODE=pull`)
```json
{
  "source": "WebhookFollow",
  "execId": "wf-1700000000000-xyz789",
  "dataVer": "43",
  "ts": 1700000000000,
  "counters": { "checked": 54, "changed": 2, "errors": 0 },
  "mode": "pull"
}
```
Nesse modo o webhook apenas sinaliza a mudança: o site deve chamar `getSnapshot` para obter o snapshot completo.

### Heartbeat opcional (`SITE_WEBHOOK_HEARTBEAT=true`)
```json
{
  "source": "WebhookFollow",
  "execId": "wf-1700000001000-heartbeat",
  "dataVer": "43",
  "ts": 1700000001000,
  "mode": "heartbeat",
  "counters": { "checked": 54, "changed": 0, "errors": 0 }
}
```

## Assinatura do webhook
Quando `SITE_WEBHOOK_SECRET` estiver setado, cada payload chega com o cabeçalho `X-WF-Signature` (HMAC SHA-256 do body). Valide o cabeçalho usando o mesmo segredo antes de aceitar o update.

## Comentários e chat no ClickUp
O `doPost`, além de disparar o pipeline completo (`sincronizarGreenMileStable`), agora deixa um comentário na task com o resultado:
1. Sucesso com mudanças – lista quantas subtasks foram patchadas e aponta as rotas com fingerprint alterado.
2. Sem mudanças – informa que a task já estava alinhada e o checkbox é limpo para aguardar o próximo loop.
3. Em paralelo, sempre que há mudança (ou heartbeat configurado) o webhook do site é disparado com `dataVer` atualizado.

## Endpoint `getSnapshot`
O servidor pode chamar `getSnapshot` para puxar o snapshot completo (ou para rotas específicas) sempre que receber um webhook pull.
- URL: `https://script.google.com/macros/s/DEPLOY_ID/exec?func=getSnapshot`
- Query opcional: `routes=6100001,6100002` limita a resposta a um subconjunto de rotas monitoradas.

### Exemplo de resposta
```json
{
  "status": "success",
  "data": {
    "dataVer": "43",
    "snapshotTs": 1700000002000,
    "items": [
      {
        "routeKey": "6100001",
        "rows": 23,
        "fingerprint": "abc123...",
        "items": [
          { "stop.location.key": "CLIENTE-A", "stop.plannedSequenceNum": 1, "stop.deliveryStatus": "DELIVERED" }
        ]
      }
    ]
  }
}
```
Os objetos em `items` seguem a mesma flatten usada para fingerprints e podem ser usados pelo front para re-renderizar com segurança.

## Propriedades persistidas
- `WF_ROUTE_FP:<route>` – fingerprint SHA-256 de cada rota.
- `WF_BATCH_DATA_VERSION` – versão incremental exibida no webhook e no `getSnapshot`.
- `WF_LAST_RUN_SUMMARY` – resumo json da última execução (contadores, status, execId).

## Observações finais
- O pipeline continua chamando `sincronizarGreenMileStable`, então o comportamento legado permanece quando `MODE=LEGACY`.
- `runnerMain` pode ser acionado manualmente via `runRunnerManual()` e usa o mesmo lock e reschedule automático.
- Telemetria/logs indicam `batchFetchCount`, `checkedRoutes`, `changedRoutes`, `patchedRoutes`, `webhookSent`, `gmErrors` e `cuErrors`.
- O heartbeat e o webhook delta não enviam tokens; somente o body JSON é logado (o secret nunca aparece nos logs).
