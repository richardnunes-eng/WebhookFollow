/**
 * ==============================================================================
 * ARQUIVO: modGreenMile.gs (VERSÃO FINAL: GM REAL + CLICKUP URL + WEBHOOK)
 * ==============================================================================
 */

// ==============================================================================
// 1. CONFIGURAÇÕES E CREDENCIAIS
// ==============================================================================


// --- CREDENCIAIS CLICKUP ---
const CLICKUP_TOKEN = "pk_87986690_9X1MC60UE18B1X9PEJFRMEFTT6GNHHFS";

// --- CREDENCIAIS GREENMILE ---
const GM_USERNAME = "richardthx";
const GM_PASSWORD = "GM@thx2025";
const GM_URL_BASE = "https://3coracoes.greenmile.com/StopView/Summary";

// --- CONFIGURAÇÕES GERAIS ---
const TAMANHO_LOTE = 120;
const DATA_MINIMA_GM = new Date("2025-12-01");
const STATUS_FINAL_KEYWORDS = ["finalizada", "done", "complete", "entregue", "retorno"];
const DURACAO_MINUTOS_NO_CLIENTE = 30;

const SHEET_NAME_MAIN = "ENTREGAS";
const SHEET_NAME_GM = "GreenMile";
const SHEET_NAME_LOG_FINALIZADOS = "GM Finalizados";
const PROPS_KEY_ROTAS_FINALIZADAS = "GM_ROTAS_FINALIZADAS";
const LOOP_CLICKUP_LIST_ID = "901314444197"; // lista monitorada pelo loop automatizado
const LOOP_MINUTES_INTERVAL = 1; // Apps Script aceita mínimo de 1 minuto (45s não é suportado)
const LOOP_FETCH_BATCH_PAGES = 4;
const LOOP_STATUS_EXCLUDE = ["cancelado", "sinistro"];
const CACHE_TTL_TASK_SECONDS = 120;
const CACHE_TTL_SUBTASK_SECONDS = 300;
const CACHE_TTL_TIMER_SECONDS = 600;
const CLICKUP_PREFETCH_BATCH_SIZE = 25;
const SNAPSHOT_KEY_PREFIX = "GM_ROUTE_SNAPSHOT:";
const SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA = "GM Snapshot EntradaSaida";
const SHEET_NAME_LOG_ENTRADA_SAIDA = "GM Log EntradaSaida";

// Cache local em memória para prefetched tasks durante o loop
var PREFETCH_TASKS_BY_ID = {};

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("GreenMile")
    .addItem("Limpar cache", "limparCacheManual")
    .addItem("Reset total (cache + propriedades)", "resetTotalCaches")
    .addSeparator()
    .addSubMenu(SpreadsheetApp.getUi().createMenu("Monitoramento (Vigia)")
      .addItem("Ativar Vigia (5min)", "configurarVigia")
      .addItem("Parar Vigia", "pararVigia")
      .addItem("Verificar Agora", "monitorarStatusAgendamento"))
    .addToUi();
}

function onInstall(e) {
  onOpen(e);
}

function limparCacheManual() {
  const detalhes = limparCacheNotificacoes();
  const rotas = detalhes.rotasFinalizadasReset ? "Rotas finalizadas resetadas" : "Rotas finalizadas mantidas";
  const mensagem = `Cache limpo: ${detalhes.snapshotKeysRemoved} snapshot(s) removido(s). ${rotas}.`;
  SpreadsheetApp.getActiveSpreadsheet().toast(mensagem, "GreenMile", 6);
}

function resetTotalCaches() {
  const props = PropertiesService.getScriptProperties();
  const cache = CacheService.getScriptCache();
  const todasProps = props.getProperties();

  const keysToDelete = [
    PROPS_KEY_ROTAS_FINALIZADAS,
    "WF_ROUTE_FP:",
    "WF_LAST_RUN_SUMMARY",
    "WF_GM_TOKEN",
    "WF_GM_TOKEN_EXPIRES",
    "WF_CLICKUP_CURSOR",
    "WF_MONITORED_ROUTES",
    "WF_BATCH_DATA_VERSION",
    "WF_RUNNER_ERROR_STREAK",
    "SITE_WEBHOOK_HEARTBEAT"
  ];

  let removedProps = 0;
  Object.keys(todasProps).forEach(key => {
    const isSnapshot = key.startsWith(SNAPSHOT_KEY_PREFIX);
    const isFingerprint = key.startsWith("WF_ROUTE_FP:");
    const isListed = keysToDelete.includes(key);
    if (isSnapshot || isFingerprint || isListed) {
      props.deleteProperty(key);
      removedProps += 1;
    }
  });

  try { cache.remove("gm_token"); } catch (e) { }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  [SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA, SHEET_NAME_LOG_ENTRADA_SAIDA].forEach(name => {
    const sheet = ss.getSheetByName(name);
    if (sheet) sheet.clear();
  });

  const mensagem = `Reset total concluído: ${removedProps} propriedade(s) removida(s).`;
  console.log(mensagem);
  SpreadsheetApp.getActiveSpreadsheet().toast(mensagem, "GreenMile", 6);
}

// --- DEFINIÇÃO DAS COLUNAS (API GreenMile + ClickUp) ---
const COLUNAS_GM = [
  "route.key",
  "clickup.taskUrl",
  "clickup.taskNfes",
  "clickup.subtaskName",
  "clickup.subtaskStatus",
  "clickup.nfes",
  "stop.plannedSequenceNum",
  "stop.actualArrival",
  "stop.actualDeparture",
  "stop.hasSignature",
  "stop.undeliverableCode.description",
  "stop.deliveryStatus",
  "stop.location.description",
  "stop.location.addressLine1",
  "stop.location.district",
  "stop.actualSize1",
  "stop.plannedSize1",
  "stop.baseLineSize1",
  "stop.actualSize2",
  "stop.plannedSize2",
  "stop.plannedSize3",
  "stop.stopType.type",
  "stop.location.city",
  "stop.location.key",
];

// --- MAPEAMENTO PARA NOMES EM PORTUGUÊS ---
const COLUNAS_PT = {
  "route.key": "Plano",
  "clickup.taskUrl": "Link ClickUp",
  "clickup.taskNfes": "NFes Rota",
  "clickup.subtaskName": "Nome Subtask",
  "clickup.subtaskStatus": "Status Subtask",
  "clickup.nfes": "NFe Cliente",
  "stop.plannedSequenceNum": "Sequência",
  "stop.actualArrival": "Chegada",
  "stop.actualDeparture": "Saída",
  "stop.hasSignature": "Assinatura",
  "stop.undeliverableCode.description": "Motivo Devolução",
  "stop.deliveryStatus": "Status Entrega",
  "stop.location.description": "Cliente",
  "stop.location.addressLine1": "Endereço",
  "stop.location.district": "Bairro",
  "stop.actualSize1": "Peso Real",
  "stop.plannedSize1": "Peso Planejado",
  "stop.baseLineSize1": "Peso Base",
  "stop.actualSize2": "Volume Real",
  "stop.plannedSize2": "Volume Planejado",
  "stop.plannedSize3": "Valor",
  "stop.stopType.type": "Tipo Parada",
  "stop.location.city": "Cidade",
  "stop.location.key": "Código Cliente",
};

// Mapeamento inverso (PT -> EN) para leitura do histórico
const COLUNAS_EN = Object.fromEntries(
  Object.entries(COLUNAS_PT).map(([en, pt]) => [pt, en])
);

// ==============================================================================
// 2. API CONTROLLER (WEBHOOKS)
// ==============================================================================

function doPost(e) {
  // === SALVA DEBUG EM PROPRIEDADE (persistente) ===
  const debugInfo = {
    timestamp: new Date().toISOString(),
    parameter: e.parameter || {},
    queryString: e.queryString || "",
    hasPostData: !!(e.postData),
    postDataType: e.postData ? e.postData.type : null,
    postDataContents: e.postData ? (e.postData.contents || "").substring(0, 2000) : null
  };
  PropertiesService.getScriptProperties().setProperty("LAST_WEBHOOK_DEBUG", JSON.stringify(debugInfo));
  console.log(`🚀 [INÍCIO] doPost - dados salvos em LAST_WEBHOOK_DEBUG`);

  const lock = LockService.getScriptLock();

  if (!lock.tryLock(10000)) {
    return ContentService.createTextOutput(JSON.stringify({ status: "error", message: "Busy" })).setMimeType(ContentService.MimeType.JSON);
  }

  try {
    // === EXTRAI PARÂMETROS DE MÚLTIPLAS FONTES ===
    let taskIdFromWebhook = null;
    let action = "SINCRONIZAR";

    // 1. Query string (?TaskID=xxx&Action=Entregas)
    if (e.parameter) {
      taskIdFromWebhook = e.parameter.TaskID || e.parameter.taskid || e.parameter.task_id || taskIdFromWebhook;
      if (e.parameter.Action) action = e.parameter.Action.toUpperCase();
    }

    // 2. Body JSON (ClickUp envia dados aqui)
    if (e.postData && e.postData.contents) {
      try {
        const body = JSON.parse(e.postData.contents);
        console.log(`📦 [BODY KEYS] ${Object.keys(body).join(", ")}`);

        // ClickUp webhook format - PRIORIZA payload.id (formato confirmado)
        if (body.payload && body.payload.id) {
          taskIdFromWebhook = body.payload.id;
          console.log(`✅ TaskID extraído de payload.id: ${taskIdFromWebhook}`);
        } else {
          // Fallback para outras estruturas
          taskIdFromWebhook = taskIdFromWebhook ||
            body.task_id ||
            body.TaskID ||
            body.taskId ||
            (body.task && body.task.id) ||
            (body.history_items && body.history_items[0] && body.history_items[0].parent_id) ||
            body.id;
        }

        if (body.Action) action = body.Action.toUpperCase();

        console.log(`🔍 [FINAL] taskId=${taskIdFromWebhook || "NENHUM"}`);
      } catch (parseErr) {
        console.log(`❌ [PARSE ERROR] ${parseErr.message}`);
        console.log(`📦 [BODY RAW] ${e.postData.contents.substring(0, 500)}`);
      }
    }

    // === RASTREAMENTO PERSISTENTE ===
    const trace = { taskId: taskIdFromWebhook, action: action, steps: [] };
    trace.steps.push("1-extração-ok");

    // Se não tem taskId, loga e retorna erro
    if (!taskIdFromWebhook) {
      trace.steps.push("2-sem-taskid");
      PropertiesService.getScriptProperties().setProperty("LAST_TRACE", JSON.stringify(trace));
      return ContentService.createTextOutput(JSON.stringify({
        status: "error",
        message: "TaskID não encontrado no webhook"
      })).setMimeType(ContentService.MimeType.JSON);
    }

    trace.steps.push("2-tem-taskid");
    let resultado = {};

    switch (action) {
      case "SINCRONIZAR":
      case "ENTREGAS":
        trace.steps.push("3-antes-sync");
        PropertiesService.getScriptProperties().setProperty("LAST_TRACE", JSON.stringify(trace));

        try {
          // skipLock=true pois doPost já tem o lock
          sincronizarGreenMileStable(taskIdFromWebhook, true);
          trace.steps.push("4-sync-ok");
        } catch (syncErr) {
          trace.steps.push("4-sync-erro: " + syncErr.message);
        }

        resultado = { mensagem: "Sincronização OK", task_id: taskIdFromWebhook };
        break;

      case "PING":
        resultado = { mensagem: "Pong! Online." };
        break;

      case "LIMPAR_CACHE":
        if (typeof limparCacheNotificacoes === 'function') {
          const detalhes = limparCacheNotificacoes();
          resultado = { mensagem: "Cache limpo.", detalhes };
        } else {
          resultado = { mensagem: "Função indisponível." };
        }
        break;

      default:
        return ContentService.createTextOutput(JSON.stringify({ status: "error", message: "Action invalida" })).setMimeType(ContentService.MimeType.JSON);
    }

    return ContentService.createTextOutput(JSON.stringify({
      status: "success",
      data: resultado
    })).setMimeType(ContentService.MimeType.JSON);

  } catch (erro) {
    console.error(`❌ [WEBHOOK] Erro: ${erro.message}`);
    return ContentService.createTextOutput(JSON.stringify({ status: "error", message: erro.message })).setMimeType(ContentService.MimeType.JSON);
  } finally {
    lock.releaseLock();
  }
}

// Função 'executarLoopListaClickUp' removida (substituída por runnerMain)
// função 'agendarLoopListaClickUp' removida
// função 'removerLoopListaClickUpTrigger' removida

/**
 * Prefetch das tasks com subtasks em lote (fetchAll) para acelerar o loop.
 * Salva em memória e em cache para reuso imediato.
 */
function prefetchTasksClickUpBatch(tasks) {
  if (!tasks || tasks.length === 0) return;

  PREFETCH_TASKS_BY_ID = {};

  const taskIds = tasks.map(t => t && t.id).filter(Boolean);
  if (taskIds.length === 0) return;

  const clickUpToken = getClickUpToken_();
  const scriptCache = CacheService.getScriptCache();
  const optionsBase = {
    method: "GET",
    headers: {
      "Authorization": clickUpToken,
      "Content-Type": "application/json"
    },
    muteHttpExceptions: true
  };

  console.log(`⚡ Prefetch ClickUp: ${taskIds.length} task(s) em lotes de ${CLICKUP_PREFETCH_BATCH_SIZE}...`);

  for (let i = 0; i < taskIds.length; i += CLICKUP_PREFETCH_BATCH_SIZE) {
    const batchIds = taskIds.slice(i, i + CLICKUP_PREFETCH_BATCH_SIZE);
    const requests = batchIds.map(taskId => ({
      url: `https://api.clickup.com/api/v2/task/${taskId}?include_subtasks=true`,
      ...optionsBase
    }));

    const responses = UrlFetchApp.fetchAll(requests);

    responses.forEach((response, idx) => {
      const taskId = batchIds[idx];
      const code = response.getResponseCode();
      if (code !== 200 && code !== 204) {
        console.warn(`⚠️ Prefetch task ${taskId} retornou HTTP ${code}`);
        return;
      }
      try {
        const json = JSON.parse(response.getContentText());
        PREFETCH_TASKS_BY_ID[taskId] = json;
        try {
          scriptCache.put(`cu_task_${taskId}`, JSON.stringify(json), CACHE_TTL_TASK_SECONDS);
        } catch (e) { }
      } catch (e) {
        console.warn(`⚠️ Falha ao parsear prefetch task ${taskId}: ${e.message}`);
      }
    });
  }
}

/**
 * Busca todas as tasks abertas da lista (sem subtasks).
 */
function buscarTasksAtivasDaLista(listId) {
  if (!listId) return [];

  const baseUrl = `https://api.clickup.com/api/v2/list/${listId}/task`;
  const results = [];
  let currentPage = 0;
  const clickUpToken = getClickUpToken_();
  const optionsBase = {
    method: "GET",
    headers: {
      "Authorization": clickUpToken,
      "Content-Type": "application/json"
    },
    muteHttpExceptions: true
  };

  while (true) {
    const pages = [];
    for (let i = 0; i < LOOP_FETCH_BATCH_PAGES; i++) {
      pages.push(currentPage + i);
    }

    const requests = pages.map(page => {
      const query = [
        "subtasks=false",
        "include_closed=false",
        "archived=false",
        "order_by=id",
        "reverse=false",
        "limit=100",
        `page=${page}`
      ].join("&");
      return {
        url: `${baseUrl}?${query}`,
        ...optionsBase
      };
    });

    const responses = UrlFetchApp.fetchAll(requests);
    let atLeastOneMore = false;

    responses.forEach((response, idx) => {
      const page = pages[idx];
      const code = response.getResponseCode();
      if (code !== 200) {
        console.warn(`⚠️ Página ${page} da lista ${listId} retornou HTTP ${code}`);
        return;
      }
      const payload = JSON.parse(response.getContentText());
      const tasks = Array.isArray(payload.tasks) ? payload.tasks : [];
      const filtered = tasks.filter(task => !deveIgnorarTaskPorStatus(task));
      console.log(`📦 Página ${page} trouxe ${filtered.length}/${tasks.length} task(s) úteis.`);
      results.push(...filtered);
      if (payload.next_page && payload.next_page.page !== null && payload.next_page.page !== undefined) {
        atLeastOneMore = true;
      }
    });

    currentPage += LOOP_FETCH_BATCH_PAGES;
    if (!atLeastOneMore) break;
  }

  return results;
}

function deveIgnorarTaskPorStatus(task) {
  if (!task || !task.status) return false;
  const statusName = typeof task.status === "string"
    ? task.status
    : (task.status.status || task.status).toString();
  const lower = statusName.toLowerCase();
  return LOOP_STATUS_EXCLUDE.some(excluir => lower.includes(excluir));
}

function doGet(e) {
  const params = e && e.parameter ? e.parameter : {};
  const action = params.action || params.Action || "";

  // Se pedir snapshot via API, retorna JSON
  if (action.toLowerCase() === "getsnapshot") {
    return getSnapshot(e);
  }

  // Se pedir status, retorna JSON
  if (action.toLowerCase() === "status") {
    return ContentService.createTextOutput(JSON.stringify({ status: "online" })).setMimeType(ContentService.MimeType.JSON);
  }

  // Por padrão, serve o dashboard HTML
  return HtmlService.createHtmlOutputFromFile("Dashboard")
    .setTitle("Dashboard de Rotas")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Função chamada pelo frontend do dashboard para buscar dados
 * Combina dados do GreenMile com informações do ClickUp
 */
function getDashboardData() {
  const scriptProps = PropertiesService.getScriptProperties();

  try {
    if (!LOOP_CLICKUP_LIST_ID) {
      throw new Error("LOOP_CLICKUP_LIST_ID não configurado.");
    }

    console.log("[Dashboard] Buscando tasks ativas...");
    const tasks = buscarTasksAtivasDaLista(LOOP_CLICKUP_LIST_ID);
    const routeMap = buildRouteMapFromTasks(tasks);
    const routeKeys = Array.from(routeMap.keys());

    console.log(`[Dashboard] ${tasks.length} tasks, ${routeKeys.length} rotas`);

    // Busca snapshots do GreenMile
    const snapshots = fetchGreenMileSnapshots(routeKeys);
    const dataVer = getCurrentDataVersion(scriptProps);

    // Para cada rota, enriquece com dados do ClickUp
    const items = routeKeys.map((routeKey) => {
      const snapshot = snapshots[routeKey] || {};
      const routeInfo = routeMap.get(routeKey) || {};
      const taskId = routeInfo.taskId;
      const taskUrl = routeInfo.taskUrl || `https://app.clickup.com/t/${taskId}`;

      // Busca subtasks do ClickUp para esta rota
      let mapaSubtasks = new Map();
      if (taskId) {
        try {
          mapaSubtasks = buscarSubtasksComIdGMGlobal(taskId);
        } catch (e) {
          console.error(`[Dashboard] Erro ao buscar subtasks de ${taskId}: ${e.message}`);
        }
      }

      // Enriquece cada item do snapshot com dados do ClickUp
      const enrichedItems = (snapshot.items || []).map(item => {
        const codigoCliente = item["stop.location.key"];
        const subtaskInfo = codigoCliente ? mapaSubtasks.get(String(codigoCliente).trim()) : null;

        // Adiciona campos do ClickUp
        const enriched = Object.assign({}, item);
        if (subtaskInfo) {
          enriched["clickup.taskUrl"] = subtaskInfo.subtaskUrl || taskUrl;
          enriched["clickup.subtaskName"] = subtaskInfo.subtaskName || "";
          enriched["clickup.subtaskStatus"] = subtaskInfo.status || "";
          enriched["clickup.nfes"] = subtaskInfo.nfes || "";
        } else {
          enriched["clickup.taskUrl"] = taskUrl;
          enriched["clickup.subtaskName"] = "";
          enriched["clickup.subtaskStatus"] = "";
          enriched["clickup.nfes"] = "";
        }

        return enriched;
      });

      return {
        routeKey,
        taskUrl,
        fingerprint: snapshot.fingerprint || null,
        rows: enrichedItems.length,
        error: snapshot.error || null,
        items: enrichedItems
      };
    });

    console.log(`[Dashboard] Retornando ${items.length} rotas com dados enriquecidos`);

    return {
      status: "success",
      data: {
        dataVer: String(dataVer),
        snapshotTs: Date.now(),
        items
      }
    };
  } catch (error) {
    console.error(`[WF] getDashboardData error: ${error.message}`);
    return {
      status: "error",
      message: error.message
    };
  }
}


/**
 * Versão global de buscarSubtasksComIdGM para uso no getDashboardData
 * Busca subtasks de uma task e extrai o Custom Field "ID GM Localização"
 * Retorna um Map: idGMLocalizacao -> { subtaskId, subtaskName, subtaskUrl, status, nfes }
 */
function buscarSubtasksComIdGMGlobal(taskId) {
  const task = buscarTaskClickUp(taskId);
  if (!task || !task.subtasks || task.subtasks.length === 0) {
    console.log(`[Dashboard] Task ${taskId} não tem subtasks`);
    return new Map();
  }

  const mapaIdGM = new Map();
  const clickUpToken = getClickUpToken_();

  // Prepara requisições para buscar subtasks em paralelo
  const requests = task.subtasks.map(subtask => ({
    url: `https://api.clickup.com/api/v2/task/${subtask.id}`,
    method: "GET",
    headers: {
      "Authorization": clickUpToken,
      "Content-Type": "application/json"
    },
    muteHttpExceptions: true
  }));

  try {
    const responses = UrlFetchApp.fetchAll(requests);

    responses.forEach((response) => {
      if (response.getResponseCode() !== 200) return;

      const subtaskCompleta = JSON.parse(response.getContentText());
      let idGM = null;

      if (subtaskCompleta.custom_fields && Array.isArray(subtaskCompleta.custom_fields)) {
        const campo = subtaskCompleta.custom_fields.find(cf =>
          cf.name && cf.name.includes("ID GM Localização")
        );

        if (campo && campo.value) {
          idGM = String(campo.value).trim();
        }
      }

      if (idGM) {
        let nfes = "";
        const campoNF = subtaskCompleta.custom_fields.find(cf =>
          cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
        );

        if (campoNF && campoNF.value !== undefined && campoNF.value !== null) {
          nfes = String(campoNF.value).trim();
        }

        const info = {
          subtaskId: subtaskCompleta.id,
          subtaskName: subtaskCompleta.name,
          subtaskUrl: `https://app.clickup.com/t/${subtaskCompleta.id}`,
          status: subtaskCompleta.status ? subtaskCompleta.status.status : "",
          nfes: nfes
        };
        mapaIdGM.set(idGM, info);
      }
    });
  } catch (e) {
    console.error(`[Dashboard] Erro ao buscar subtasks: ${e.message}`);
  }

  console.log(`[Dashboard] Total subtasks com ID GM: ${mapaIdGM.size}`);
  return mapaIdGM;
}

function getSnapshot(e) {

  const scriptProps = PropertiesService.getScriptProperties();
  try {
    const params = e && e.parameter ? e.parameter : {};
    const rawRoutes = params.routes || params.routeKeys || "";
    let routeKeys = rawRoutes
      .split(",")
      .map((value) => String(value || "").trim())
      .filter((value) => value);

    if (routeKeys.length === 0) {
      if (!LOOP_CLICKUP_LIST_ID) {
        throw new Error("LOOP_CLICKUP_LIST_ID não configurado para build de snapshot.");
      }
      const tasks = buscarTasksAtivasDaLista(LOOP_CLICKUP_LIST_ID);
      routeKeys = Array.from(buildRouteMapFromTasks(tasks).keys());
    }

    const snapshots = fetchGreenMileSnapshots(routeKeys);
    const dataVer = getCurrentDataVersion(scriptProps);
    const items = routeKeys.map((routeKey) => {
      const snapshot = snapshots[routeKey] || {};
      return {
        routeKey,
        fingerprint: snapshot.fingerprint || null,
        rows: snapshot.rows || 0,
        error: snapshot.error || null,
        items: snapshot.items || []
      };
    });

    return ContentService.createTextOutput(
      JSON.stringify({
        status: "success",
        data: {
          dataVer: String(dataVer),
          snapshotTs: Date.now(),
          items
        }
      })
    ).setMimeType(ContentService.MimeType.JSON);
  } catch (error) {
    console.error(`[WF] getSnapshot error: ${error.message}`);
    return ContentService.createTextOutput(
      JSON.stringify({
        status: "error",
        message: error.message
      })
    ).setMimeType(ContentService.MimeType.JSON);
  }
}

// === VER TRACE DA ÚLTIMA EXECUÇÃO ===
function verTrace() {
  const trace = PropertiesService.getScriptProperties().getProperty("LAST_TRACE");
  if (trace) {
    const parsed = JSON.parse(trace);
    console.log("=== TRACE DA ÚLTIMA EXECUÇÃO ===");
    console.log("TaskID: " + parsed.taskId);
    console.log("Action: " + parsed.action);
    console.log("Steps: " + parsed.steps.join(" → "));
  } else {
    console.log("Nenhum trace encontrado.");
  }
}

// === LIMPAR LOCK TRAVADO ===
function limparLock() {
  const lock = LockService.getScriptLock();
  if (lock.hasLock()) {
    lock.releaseLock();
    console.log("🔓 Lock liberado!");
  } else {
    console.log("✅ Nenhum lock ativo.");
  }
}

// === VER DEBUG DO ÚLTIMO WEBHOOK ===
function verUltimoWebhook() {
  const debug = PropertiesService.getScriptProperties().getProperty("LAST_WEBHOOK_DEBUG");
  if (debug) {
    const parsed = JSON.parse(debug);
    console.log("=== ÚLTIMO WEBHOOK RECEBIDO ===");
    console.log("Timestamp: " + parsed.timestamp);
    console.log("Query String: " + parsed.queryString);
    console.log("Parameters: " + JSON.stringify(parsed.parameter, null, 2));
    console.log("PostData Type: " + parsed.postDataType);
    console.log("PostData Contents: " + parsed.postDataContents);

    // Tenta extrair o TaskID pra mostrar
    if (parsed.postDataContents) {
      try {
        const body = JSON.parse(parsed.postDataContents);
        console.log("--- EXTRAÇÃO ---");
        console.log("body.payload existe: " + !!(body.payload));
        console.log("body.payload.id: " + (body.payload ? body.payload.id : "N/A"));
      } catch (e) {
        console.log("Erro ao parsear body: " + e.message);
      }
    }
  } else {
    console.log("Nenhum webhook recebido ainda.");
  }
}

// === FUNÇÃO DE TESTE (roda pelo editor) ===
function testeManual() {
  console.log("🧪 Iniciando teste manual...");

  // Simula um webhook com TaskID real
  const fakeEvent = {
    parameter: {
      TaskID: "86aeujcq3",  // TaskID real do ClickUp
      Action: "Entregas"
    },
    queryString: "TaskID=86aerzkgv&Action=Entregas"
  };

  console.log("🧪 Chamando doPost com evento fake...");
  const resultado = doPost(fakeEvent);
  console.log("🧪 Resultado: " + resultado.getContent());
}

// ==============================================================================
// 3. LÓGICA PRINCIPAL (SINCRONIZAÇÃO)
// ==============================================================================

function sincronizarGreenMileStable(taskIdFromWebhook, skipLock) {
  console.time("⏱️ Tempo Total GM");
  console.log("🔄 Iniciando Sincronização...");

  // Se skipLock=true, não tenta adquirir lock (já foi adquirido pelo caller)
  if (skipLock) {
    try {
      executarSincronizacaoGreenMile(taskIdFromWebhook);
    } finally {
      console.timeEnd("⏱️ Tempo Total GM");
    }
    return;
  }

  // Lock apenas quando chamado diretamente (loop, teste manual, etc.)
  const lock = LockService.getScriptLock();
  try {
    if (!lock.tryLock(5000)) return; // Evita duplicidade local
    executarSincronizacaoGreenMile(taskIdFromWebhook);
  } finally {
    lock.releaseLock();
    console.timeEnd("⏱️ Tempo Total GM");
  }
}

function executarSincronizacaoGreenMile(taskIdFromWebhook) {
  const scriptCache = CacheService.getScriptCache();
  const scriptProps = PropertiesService.getScriptProperties();
  const mapaEntradaSaida = lerMapaEntradasSaidas();
  const logAlteracoesEntradaSaida = [];
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let houveMudancas = false;
  let wsMain = ss.getSheetByName(SHEET_NAME_MAIN);
  let wsOut = ss.getSheetByName(SHEET_NAME_GM);

  if (!wsMain) {
    console.log(`📝 Criando aba '${SHEET_NAME_MAIN}'...`);
    wsMain = ss.insertSheet(SHEET_NAME_MAIN);
    // Cria headers básicos
    wsMain.getRange(1, 1, 1, 3).setValues([["PLANO", "DATA DE CRIAÇÃO", "STATUS"]]);
    wsMain.getRange(1, 1, 1, 3).setFontWeight("bold");
  }
  if (!wsOut) {
    console.log(`📝 Criando aba '${SHEET_NAME_GM}'...`);
    wsOut = ss.insertSheet(SHEET_NAME_GM);
  }

  // === 0. BUSCAR ROTA E SUBTASKS NO CLICKUP (se veio TaskID) ===
  let rotaDoWebhook = null;
  let mapaSubtasks = new Map(); // idGMLocalizacao -> { subtaskId, subtaskName, subtaskUrl, status, ... }
  let nfesTaskPrincipal = ""; // NFes do campo personalizado da task principal
  let idCampoQtdEntregas = null; // ID do campo para atualizar no final
  let planejadasQtdEntregas = null;
  let statusTaskPrincipalAtual = "";
  let estaCritico = false;
  let mantemCritico = false;
  let rotaAtrasada = false;
  let isReentregaPura = false;
  let temReentrega = false;
  let taskClickUp = null;
  let totalSubtasksCount = 0;
  let rotaNoCliente = false;
  let ultimaChegadaNoCliente = null;
  const subtasksSemSaida = new Map();

  if (taskIdFromWebhook) {
    console.log(`📌 Task ID do Webhook: ${taskIdFromWebhook}`);

    // Busca a task principal
    taskClickUp = buscarTaskClickUp(taskIdFromWebhook);

    if (taskClickUp) {
      totalSubtasksCount = Array.isArray(taskClickUp.subtasks) ? taskClickUp.subtasks.length : 0;

      rotaDoWebhook = taskClickUp.name;
      // Remove o sufixo -numero se tiver, para ficar só a rota base
      rotaDoWebhook = rotaDoWebhook.includes("-") ? rotaDoWebhook.split("-")[0].trim() : rotaDoWebhook.trim();
      console.log(`📌 Rota extraída do ClickUp: ${rotaDoWebhook}`);
      isReentregaPura = !rotaDoWebhook || !rotaDoWebhook.startsWith("610");

      statusTaskPrincipalAtual = taskClickUp.status ? taskClickUp.status.status : "";
      estaCritico = isStatusCritico(statusTaskPrincipalAtual);

      // Extrai o campo "📄 Notas fiscais" da task principal
      let campoNF_Principal = null;
      if (taskClickUp.custom_fields && Array.isArray(taskClickUp.custom_fields)) {
        // Busca por "Notas fiscais", "NFe" ou similar
        campoNF_Principal = taskClickUp.custom_fields.find(cf =>
          cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
        );

        if (campoNF_Principal) {
          // No ClickUp, o valor pode estar em campoNF_Principal.value
          let valorRaw = campoNF_Principal.value;
          if (valorRaw !== undefined && valorRaw !== null) {
            nfesTaskPrincipal = String(valorRaw).trim();
            console.log(`✅ Campo encontrado: "${campoNF_Principal.name}" | Valor: "${nfesTaskPrincipal.substring(0, 30)}..."`);
          } else {
            console.log(`⚠️ Campo "${campoNF_Principal.name}" encontrado, mas o valor (value) está vazio/nulo.`);
          }
        } else {
          console.log(`❌ Campo de Notas Fiscais não identificado na task principal.`);
        }
      }

      // Busca as subtasks com o ID GM Localização (passa a task já buscada para evitar chamada duplicada)
      mapaSubtasks = buscarSubtasksComIdGM(taskIdFromWebhook, taskClickUp);

      // Subtasks sem ID GM Localização -> Reentrega (reentrega dentro do plano)
      if (taskClickUp && Array.isArray(taskClickUp.subtasks)) {
        const houveReentregaSemId = aplicarReentregaSubtasksSemIdGM(taskClickUp, mapaSubtasks);
        if (houveReentregaSemId) temReentrega = true;
      }

      // === AGREGAÇÃO E ATUALIZAÇÃO CLICKUP ===
      if (mapaSubtasks.size > 0) {
        const todasNfes = [];
        mapaSubtasks.forEach(sub => {
          if (sub.nfes) {
            todasNfes.push(sub.nfes);
            // Se a subtask tem NFes lidas do checklist mas NÃO tem no custom field, atualiza
            if (sub.needsUpdate && sub.fieldId) {
              atualizarCampoPersonalizadoClickUp(sub.subtaskId, sub.fieldId, sub.nfes);
            }
          }
        });

        // Se a task principal não tem NFes (no custom field), mas temos agregadas das subtasks, atualiza
        if (!nfesTaskPrincipal && todasNfes.length > 0) {
          nfesTaskPrincipal = todasNfes.join(", ");
          console.log(`📊 NFes agregadas: ${nfesTaskPrincipal.substring(0, 50)}...`);

          if (campoNF_Principal && campoNF_Principal.id) {
            atualizarCampoPersonalizadoClickUp(taskIdFromWebhook, campoNF_Principal.id, nfesTaskPrincipal);
          }
        }
      }

      // === CÁLCULO DE ENTREGAS (Setup) ===
      // Busca o ID do campo "🚚 QTD. de Entregas" para usar depois
      if (taskClickUp.custom_fields && Array.isArray(taskClickUp.custom_fields)) {
        const cfQtd = taskClickUp.custom_fields.find(cf =>
          cf.name && (cf.name.includes("QTD. de Entregas") || cf.name.includes("Entregas") || cf.name.includes("🚚"))
        );
        if (cfQtd) {
          idCampoQtdEntregas = cfQtd.id;
          const rawPlanejado = String(cfQtd.value || "").trim();
          const match = rawPlanejado.match(/^(\d+)/);
          const valorPlanejado = match ? Number(match[1]) : NaN;
          if (!Number.isNaN(valorPlanejado) && valorPlanejado > 0) {
            planejadasQtdEntregas = valorPlanejado;
          }
        }
      }
    }
  }

  // === 1. CARREGAR HISTÓRICO ===
  let dadosHistorico = [];
  let rotasNoBanco = new Set();
  let rotasPendentesNoBanco = new Set();
  let linhasOriginais = 0;

  const rangeGM = wsOut.getDataRange();
  const valuesGM = rangeGM.getValues();
  const rotasFinalizadasCache = obterRotasFinalizadasCache();

  if (valuesGM.length > 1) {
    linhasOriginais = valuesGM.length - 1;
    const headersGM = valuesGM[0];

    // === DEBUG: Headers do histórico ===
    console.log(`🐛 [DEBUG] Headers GreenMile: ${JSON.stringify(headersGM)}`);

    // Aceita tanto PT ("Plano", "Saída") quanto EN ("route.key", "stop.actualDeparture")
    const idxKeyGM = headersGM.findIndex(h => ["route.key", "plano", "rota"].includes(String(h).trim().toLowerCase()));
    const idxDepGM = headersGM.findIndex(h => ["stop.actualdeparture", "saída"].includes(String(h).trim().toLowerCase()));

    console.log(`🐛 [DEBUG] idxKeyGM: ${idxKeyGM} | idxDepGM: ${idxDepGM}`);

    if (idxKeyGM !== -1) {
      dadosHistorico = valoresParaObjetos(valuesGM);
      console.log(`🐛 [DEBUG] dadosHistorico.length: ${dadosHistorico.length}`);

      if (idxDepGM !== -1) {
        for (let i = 1; i < valuesGM.length; i++) {
          let rKey = String(valuesGM[i][idxKeyGM]).trim();
          let departure = valuesGM[i][idxDepGM];
          if (rKey) rotasNoBanco.add(rKey);
          if (rKey && (!departure || String(departure).trim() === "")) rotasPendentesNoBanco.add(rKey);
        }
      }
      console.log(`🐛 [DEBUG] rotasNoBanco: ${rotasNoBanco.size} | rotasPendentes: ${rotasPendentesNoBanco.size}`);
    } else {
      console.log(`🐛 [DEBUG] Coluna de rota não encontrada no histórico!`);
    }
  } else {
    console.log(`🐛 [DEBUG] Planilha GreenMile vazia ou só tem header`);
  }

  // === 2. DEFINIR ROTAS A BAIXAR ===
  let rotasParaBaixar = new Set();
  const rotasPlanejadas = new Map();

  if (isReentregaPura) {
    console.log("❗ Rota identificada como reentrega pura; pulando download GreenMile.");
  } else {
    // Se veio rota do webhook, usa ela diretamente
    if (rotaDoWebhook && String(rotaDoWebhook).startsWith("610")) {
      rotasParaBaixar.add(rotaDoWebhook);
      console.log(`📌 Usando rota do webhook: ${rotaDoWebhook}`);
    } else {
      const dataMain = wsMain.getDataRange().getValues();
      const headersMain = dataMain[0] || [];

      // === DEBUG: Headers da aba principal ===
      console.log(`🐛 [DEBUG] Headers ENTREGAS: ${JSON.stringify(headersMain)}`);

      const colNomeIdx = headersMain.findIndex(h => ["NOME", "PLANO", "ROTA"].includes(String(h).toUpperCase().trim()));
      const colDataIdx = headersMain.findIndex(h => ["DATA DE CRIAÇÃO", "DATA DE SAÍDA", "DATA DE SAIDA"].includes(String(h).toUpperCase().trim()));

      console.log(`🐛 [DEBUG] colNomeIdx: ${colNomeIdx} | colDataIdx: ${colDataIdx}`);

      if (colNomeIdx !== -1) {
        // Senão, busca na aba ENTREGAS (comportamento original)
        for (let i = 1; i < dataMain.length; i++) {
          if (colDataIdx !== -1) {
            let dataRota = dataMain[i][colDataIdx];
            if (dataRota instanceof Date && dataRota < DATA_MINIMA_GM) continue;
          }

          let val = String(dataMain[i][colNomeIdx] || "");
          if (val.length < 2) continue;

          let rotaKeyBase = val.includes("-") ? val.split("-")[0].trim() : val;
          if (!String(rotaKeyBase).startsWith("610")) continue;
          if (rotasFinalizadasCache.has(rotaKeyBase)) {
            console.log(`🟢 Rota ${rotaKeyBase} já finalizada (log) -> pulando download.`);
            continue;
          }

          if (!rotasNoBanco.has(rotaKeyBase)) {
            rotasParaBaixar.add(rotaKeyBase);
          } else if (rotasPendentesNoBanco.has(rotaKeyBase)) {
            rotasParaBaixar.add(rotaKeyBase);
          }
        }
      } else {
        console.warn("⚠️ Sem rota do webhook e coluna PLANO não encontrada.");
      }
    }
  }
  const listaDownload = Array.from(rotasParaBaixar);
  console.log(`📥 Rotas para baixar: ${listaDownload.length}`);

  // === 3. DOWNLOAD & CLICKUP URL ===
  let novosRegistros = [];
  let rotasBaixadasComSucesso = new Set();

  if (listaDownload.length > 0) {
    let token = null;
    try {
      token = getGreenMileToken_();
      console.log("🐛 [DEBUG] Token GreenMile disponível.");
    } catch (tokenError) {
      console.error(`❌ Falha ao obter token GreenMile: ${tokenError.message}`);
    }
    if (token) {
      for (let i = 0; i < listaDownload.length; i += TAMANHO_LOTE) {
        const loteAtual = listaDownload.slice(i, i + TAMANHO_LOTE);
        const requests = loteAtual.map(rota => prepararRequest(token, rota, COLUNAS_GM));

        try {
          const responses = gmFetchBatch_("gm-rotas", requests);
          console.log(`🐛 [DEBUG] Lote processado: ${loteAtual.length} rotas`);

          responses.forEach((res, index) => {
            const rotaReferencia = loteAtual[index];
            console.log(`🐛 [DEBUG] Rota ${rotaReferencia}: HTTP ${res.getResponseCode()}`);

            if (res.getResponseCode() === 200) {
              try {
                let json = JSON.parse(res.getContentText());
                let items = json.content || json.rows || json.items || json;
                const itemsList = Array.isArray(items) ? items : [];

                if (itemsList.length === 0) {
                  console.log(`🐛 [LOG] Rota ${rotaReferencia} sem registros → removendo histórico.`);
                  limparMapaParaRota(mapaEntradaSaida, rotaReferencia);
                  rotasBaixadasComSucesso.add(rotaReferencia);
                  rotasPlanejadas.set(rotaReferencia, 0);
                  houveMudancas = true;
                  return;
                }

                const flatItems = itemsList.map(item => {
                  if (!item["route.key"]) item["route.key"] = rotaReferencia;
                  let flatItem = flattenObject(item, "");
                  let rawInfo = flatItem["stop.ordersInfo"];
                  flatItem["stop.orders.number"] = rawInfo ? String(rawInfo).replace(/[\[\]\"]/g, '') : "";
                  flatItem["stop.plannedSize3"] = parseNumeroSeguro(flatItem["stop.plannedSize3"] ?? flatItem["stop.plannedSize3.value"] ?? flatItem["stop.plannedSize3.amount"]);
                  flatItem["stop.plannedSize1"] = parseNumeroSeguro(flatItem["stop.plannedSize1"] || 0);
                  flatItem["stop.plannedSequenceNum"] = parseInt(flatItem["stop.plannedSequenceNum"] || 0);
                  return flatItem;
                });
                const fingerprint = computeFingerprint(rotaReferencia, flatItems);
                const previousFingerprint = getRouteFingerprint(rotaReferencia);
                rotasBaixadasComSucesso.add(rotaReferencia);
                rotasPlanejadas.set(rotaReferencia, itemsList.length);

                if (previousFingerprint && previousFingerprint === fingerprint) {
                  console.log(`🐛 [WF] Rota ${rotaReferencia} sem mudanças (fingerprint).`);
                  return;
                }

                const entradaSaidaMudou = detectarMudancasEntradaSaida(flatItems, rotaReferencia, mapaEntradaSaida, logAlteracoesEntradaSaida);
                if (!entradaSaidaMudou) {
                  console.log(`🐛 [LOG] Rota ${rotaReferencia} sem alterações de entrada/saída.`);
                  return;
                }

                houveMudancas = true;
                console.log(`🐛 [DEBUG] Rota ${rotaReferencia}: ${itemsList.length} itens`);

                const clickupUrlFinal = taskIdFromWebhook
                  ? `https://app.clickup.com/t/${taskIdFromWebhook}`
                  : "";

                if (clickupUrlFinal) {
                  console.log(`   🔗 Link ClickUp: ${clickupUrlFinal}`);
                }

                const statusUpdates = [];
                const gmClientIds = new Set();

                flatItems.forEach(flatItem => {
                  const codigoCliente = flatItem["stop.location.key"];
                  if (codigoCliente !== undefined && codigoCliente !== null) {
                    gmClientIds.add(String(codigoCliente).trim());
                  }
                  const subtaskInfo = mapaSubtasks.get(codigoCliente);

                  if (subtaskInfo) {
                    flatItem["clickup.taskUrl"] = subtaskInfo.subtaskUrl;
                    flatItem["clickup.taskNfes"] = nfesTaskPrincipal;
                    flatItem["clickup.subtaskName"] = subtaskInfo.subtaskName;
                    flatItem["clickup.subtaskStatus"] = subtaskInfo.status;
                    flatItem["clickup.nfes"] = subtaskInfo.nfes || "";
                    console.log(`   🔗 Cliente ${codigoCliente} -> Subtask: ${subtaskInfo.subtaskName}`);

                    let novoStatus = determinarStatusSubtask(flatItem);
                    const chegouSemSaida = chegadaSemSaida(flatItem);
                    const atrasado = chegouSemSaida && chegadaAtrasada(flatItem["stop.actualArrival"]);
                    if (atrasado) rotaAtrasada = true;
                    if (chegouSemSaida) {
                      novoStatus = "No Cliente";
                      rotaNoCliente = true;
                      const descricaoCliente = subtaskInfo.subtaskName || flatItem["stop.location.description"] || `Cliente ${codigoCliente}`;
                      subtasksSemSaida.set(subtaskInfo.subtaskId, descricaoCliente);
                      ultimaChegadaNoCliente = flatItem["stop.actualArrival"] || ultimaChegadaNoCliente;
                    } else {
                      subtasksSemSaida.delete(subtaskInfo.subtaskId);
                    }
                    console.log(`   🐛 [STATUS] Cliente ${codigoCliente}: saída="${flatItem["stop.actualDeparture"] || "N/A"}" | deliveryStatus="${flatItem["stop.deliveryStatus"] || "N/A"}" | motivo="${flatItem["stop.undeliverableCode.description"] || "N/A"}" | novoStatus="${novoStatus || "null"}" | statusAtual="${subtaskInfo.status}"`);
                    if (novoStatus && novoStatus !== subtaskInfo.status) {
                      statusUpdates.push({
                        subtaskId: subtaskInfo.subtaskId,
                        novoStatus: novoStatus,
                        flatItem: flatItem,
                        subtaskInfo: subtaskInfo
                      });
                    }
                  } else {
                    flatItem["clickup.taskUrl"] = clickupUrlFinal;
                    flatItem["clickup.taskNfes"] = nfesTaskPrincipal;
                    flatItem["clickup.subtaskName"] = "";
                    flatItem["clickup.subtaskStatus"] = "";
                    flatItem["clickup.nfes"] = "";
                  }

                  novosRegistros.push(flatItem);
                });

                if (statusUpdates.length > 0) {
                  console.log(`🔄 Atualizando ${statusUpdates.length} status em paralelo...`);
                  atualizarStatusEmBatch(statusUpdates);
                }

                if (mapaSubtasks.size > 0 && gmClientIds.size > 0) {
                  const houveReentregaForaGM = aplicarReentregaSubtasksForaGM(mapaSubtasks, gmClientIds);
                  if (houveReentregaForaGM) temReentrega = true;
                }

                if (fingerprint) {
                  setRouteFingerprint(rotaReferencia, fingerprint);
                  markPlanChanged();
                }
              } catch (e) { console.warn(`Erro parse: ${e.message}`); }
            }
          });
        } catch (e) { console.error(`Erro lote: ${e.message}`); }
        if (i + TAMANHO_LOTE < listaDownload.length) Utilities.sleep(100);
      }
    }
  }

  if (!houveMudancas) {
    console.log("🔕 Nenhuma alteração de rota detectada; mantendo os dados atuais.");
    return;
  }

  if (logAlteracoesEntradaSaida.length > 0) {
    gravarLogEntradaSaida(logAlteracoesEntradaSaida);
  }
  salvarMapaEntradasSaidas(mapaEntradaSaida);

  if (rotaAtrasada && taskIdFromWebhook && taskClickUp) {
    estaCritico = true;
    const statusAtual = String(statusTaskPrincipalAtual || "").toLowerCase();
    if (!statusAtual.includes("critico") && !isStatusFinal(statusTaskPrincipalAtual)) {
      atualizarStatusTaskClickUp(taskIdFromWebhook, "Crítico");
      statusTaskPrincipalAtual = "Crítico";
    }
    mantemCritico = true;
  }

  if (rotaNoCliente && taskIdFromWebhook && taskClickUp) {
    if (!estaCritico && !isStatusFinal(statusTaskPrincipalAtual)) {
      atualizarStatusTaskClickUp(taskIdFromWebhook, "No Cliente");
      statusTaskPrincipalAtual = "No Cliente";
    } else {
      mantemCritico = true;
    }
    ajustarDatasNoCliente(taskIdFromWebhook, ultimaChegadaNoCliente);
  }

  // Se não há mais ninguém no cliente E status atual é Crítico/No Cliente → volta para "Em rota"
  if (subtasksSemSaida.size === 0 && taskIdFromWebhook && taskClickUp && !isStatusFinal(statusTaskPrincipalAtual)) {
    const statusLower = String(statusTaskPrincipalAtual || "").toLowerCase();
    const precisaVoltarEmRota = statusLower.includes("critico") ||
      statusLower.includes("crítico") ||
      statusLower.includes("no cliente");
    if (precisaVoltarEmRota) {
      console.log(`🔄 Saiu do cliente/crítico → voltando para "Em rota"`);
      atualizarStatusTaskClickUp(taskIdFromWebhook, "Em rota");
      statusTaskPrincipalAtual = "Em rota";
    }
  }

  if (subtasksSemSaida.size > 0 && taskClickUp) {
    const teamId = obterTeamId(taskClickUp);
    subtasksSemSaida.forEach((descricao, subtaskId) => {
      iniciarTimerClickUp(subtaskId, teamId, descricao);
    });
  }

  if (isReentregaPura) {
    const reentregas = totalSubtasksCount || mapaSubtasks.size;
    const valorReentregas = `${reentregas} reentreg${reentregas > 1 ? "as" : "a"}`;
    try {
      const nfesReentrega = coletarNfesReentrega(taskIdFromWebhook, taskClickUp);
      if (!nfesTaskPrincipal && nfesReentrega) {
        nfesTaskPrincipal = nfesReentrega;
        if (taskClickUp && taskClickUp.custom_fields) {
          const campoNF = taskClickUp.custom_fields.find(cf =>
            cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
          );
          if (campoNF && campoNF.id) {
            atualizarCampoPersonalizadoClickUp(taskIdFromWebhook, campoNF.id, nfesTaskPrincipal);
          }
        }
      }
    } catch (e) {
      console.warn(`⚠️ Falha ao coletar NFes reentrega: ${e.message}`);
    }

    // === FIX REENTREGA PURA ===
    // Se é reentrega pura, todas as subtasks identificadas devem ir para "Reentrega"
    if (mapaSubtasks.size > 0) {
      const updatesReentrega = [];
      mapaSubtasks.forEach(sub => {
        // Atualiza NFes da subtask se necessário
        if (sub.nfes && sub.fieldId && sub.needsUpdate) {
          atualizarCampoPersonalizadoClickUp(sub.subtaskId, sub.fieldId, sub.nfes);
        }
        // Força status Reentrega
        updatesReentrega.push({
          subtaskId: sub.subtaskId,
          novoStatus: "Reentrega"
        });
      });
      if (updatesReentrega.length > 0) {
        console.log(`📌 Atualizando ${updatesReentrega.length} subtasks para 'Reentrega' (Reentrega Pura)...`);
        atualizarStatusEmBatch(updatesReentrega);
      }
    }

    if (idCampoQtdEntregas) {
      atualizarCampoPersonalizadoClickUp(taskIdFromWebhook, idCampoQtdEntregas, valorReentregas);
    }
    if (taskClickUp && taskIdFromWebhook && !isStatusFinal(statusTaskPrincipalAtual)) {
      atualizarStatusTaskClickUp(taskIdFromWebhook, "Reentrega");
      statusTaskPrincipalAtual = "Reentrega";
    }
    console.log("📌 Caso de reentrega pura: usando dados do ClickUp sem tocar no GreenMile.");
    return;
  }

  // === 3.1 CÁLCULO FINAL DE ENTREGAS (PÓS-UPDATE) ===
  if (idCampoQtdEntregas && mapaSubtasks.size > 0 && taskIdFromWebhook) {
    const totalEntregas = taskClickUp ? totalSubtasksCount || mapaSubtasks.size : mapaSubtasks.size;
    let entregasRealizadas = 0;

    mapaSubtasks.forEach(sub => {
      const statusLower = String(sub.status || "").toLowerCase();
      // Considera FINALIZADA ou RETORNO como processado
      if (statusLower.includes("finalizada") ||
        statusLower.includes("done") ||
        statusLower.includes("complete") ||
        statusLower.includes("entregue") ||
        statusLower.includes("retorno")) { // Incluído RETORNO conforme pedido
        entregasRealizadas++;
      }
    });

    const plannedEntregas = rotasPlanejadas.get(rotaDoWebhook) || planejadasQtdEntregas || totalEntregas;
    const reentregas = Math.max(0, totalEntregas - plannedEntregas);
    const suffixReentrega = reentregas > 0 ? ` +${reentregas} reentreg${reentregas > 1 ? "as" : "a"}` : "";
    const valorQtd = `${entregasRealizadas}/${plannedEntregas}${suffixReentrega}`;
    console.log(`🚚 [FINAL] Atualizando QTD Entregas: ${valorQtd} (Field ID: ${idCampoQtdEntregas})`);

    atualizarCampoPersonalizadoClickUp(taskIdFromWebhook, idCampoQtdEntregas, valorQtd);
  }

  // === 4. MERGE E SALVAR ===
  console.log(`🐛 [DEBUG] novosRegistros: ${novosRegistros.length}`);
  console.log(`🐛 [DEBUG] rotasBaixadasComSucesso: ${Array.from(rotasBaixadasComSucesso).join(", ")}`);

  let dadosFinais = dadosHistorico.filter(d => !rotasBaixadasComSucesso.has(String(d["route.key"]).trim()));
  console.log(`🐛 [DEBUG] dadosFinais após filtro: ${dadosFinais.length}`);

  dadosFinais = dadosFinais.concat(novosRegistros);
  console.log(`🐛 [DEBUG] dadosFinais após merge: ${dadosFinais.length}`);

  const rotaFinalizada = (
    rotaDoWebhook &&
    rotaConcluidaPorSubtasks(mapaSubtasks) &&
    rotaComChegadasESaidasCompletas(dadosFinais, rotaDoWebhook)
  );

  if (rotaFinalizada) {
    const rotaTrim = String(rotaDoWebhook).trim();
    console.log(`ðŸ’¸ Rota '${rotaTrim}' finalizada -> removendo da planilha '${SHEET_NAME_GM}'.`);
    dadosFinais = dadosFinais.filter(r => String(r["route.key"] || "").trim() !== rotaTrim);
    registrarRotaFinalizada(rotaTrim, taskIdFromWebhook);
  }

  if (rotaFinalizada && taskIdFromWebhook && taskClickUp) {
    const lowerStatus = String(statusTaskPrincipalAtual || "").toLowerCase();
    if (temReentrega) {
      if (!lowerStatus.includes("reentrega") && !isStatusFinal(statusTaskPrincipalAtual)) {
        console.log(`🔁 Rota finalizada com reentrega -> atualizando task ${taskIdFromWebhook} para status "Reentrega".`);
        atualizarStatusTaskClickUp(taskIdFromWebhook, "Reentrega");
        statusTaskPrincipalAtual = "Reentrega";
      }
    } else {
      const statusIsRetorno = lowerStatus.includes("retorno");
      const podeAtualizar = !isStatusFinal(statusTaskPrincipalAtual) || statusIsRetorno;
      if (!lowerStatus.includes("validar finaliza") && podeAtualizar) {
        console.log(`🔁 Rota finalizada -> atualizando task ${taskIdFromWebhook} para status "Validar Finalização".`);
        atualizarStatusTaskClickUp(taskIdFromWebhook, "Validar Finalização");
        statusTaskPrincipalAtual = "Validar Finalização";
      }
    }
  }

  if (dadosFinais.length === 0 && linhasOriginais > 0 && !rotaFinalizada) {
    console.log(`🐛 [DEBUG] Saindo sem salvar - dados vazios mas tinha histórico`);
    return;
  }

  // Prepara Matriz
  let headersSet = new Set();
  COLUNAS_GM.forEach(c => headersSet.add(c));
  dadosFinais.forEach(r => Object.keys(r).forEach(k => { if (k !== "stop.ordersInfo") headersSet.add(k); }));

  let headerArr = Array.from(headersSet);
  console.log(`🐛 [DEBUG] headerArr (EN): ${JSON.stringify(headerArr)}`);

  // Traduz headers para português
  let headersPT = headerArr.map(h => COLUNAS_PT[h] || h);
  console.log(`🐛 [DEBUG] headersPT: ${JSON.stringify(headersPT)}`);

  let matriz = [headersPT];

  dadosFinais.forEach(item => {
    let row = headerArr.map(h => {
      let val = item[h];
      if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}T/)) return new Date(val);
      return (val === undefined || val === null) ? "" : val;
    });
    matriz.push(row);
  });

  console.log(`💾 Salvando ${matriz.length - 1} registros...`);

  try {
    const numRows = matriz.length;
    wsOut.getRange(1, 1, numRows, matriz[0].length).setValues(matriz);

    const totalLinhas = wsOut.getMaxRows();
    if (totalLinhas > numRows) {
      wsOut.getRange(numRows + 1, 1, totalLinhas - numRows, wsOut.getMaxColumns()).clearContent();
    }

    wsOut.getRange(1, 1, 1, matriz[0].length).setFontWeight("bold").setBackground("#f3f3f3");
    console.log("✅ Sincronização concluída!");

    try { if (typeof refreshDashboardCache === 'function') refreshDashboardCache(); } catch (e) { }

  } catch (e) { console.error(`Erro escrita: ${e.message}`); }


  // ==============================================================================
  // 4. FUNÇÕES AUXILIARES

  function getCacheJson(key) {
    try {
      const raw = scriptCache.get(key);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function setCacheJson(key, value, ttlSeconds) {
    try {
      scriptCache.put(key, JSON.stringify(value), ttlSeconds);
    } catch (e) { }
  }

  // ==============================================================================

  /**
   * Busca a task no ClickUp e retorna nome + subtasks com custom fields
   */
  function buscarTaskClickUp(taskId) {
    if (!taskId) return null;

    const cacheKey = `cu_task_${taskId}`;
    if (PREFETCH_TASKS_BY_ID && PREFETCH_TASKS_BY_ID[taskId]) {
      const prefetched = PREFETCH_TASKS_BY_ID[taskId];
      setCacheJson(cacheKey, prefetched, CACHE_TTL_TASK_SECONDS);
      return prefetched;
    }
    const cached = getCacheJson(cacheKey);
    if (cached) {
      return cached;
    }

    const url = `https://api.clickup.com/api/v2/task/${taskId}?include_subtasks=true`;
    try {
      console.log(`🔍 Buscando task ${taskId} no ClickUp...`);
      const response = cuFetch_("cu-task", url, { method: "GET" });
      const code = response.getResponseCode();

      console.log(`🐛 [DEBUG] ClickUp response code: ${code}`);

      if (code === 200 || code === 204) {
        const json = JSON.parse(response.getContentText());
        console.log(`✅ Task encontrada: "${json.name}"`);
        console.log(`🐛 [DEBUG] Subtasks: ${json.subtasks ? json.subtasks.length : 0}`);
        setCacheJson(cacheKey, json, CACHE_TTL_TASK_SECONDS);
        return json;
      } else {
        console.warn(`⚠️ ClickUp retornou ${code}: ${response.getContentText()}`);
      }
    } catch (e) {
      console.error(`❌ Erro ao buscar task no ClickUp: ${e.message}`);
    }

    return null;
  }

  /**
   * Busca o nome da task no ClickUp (mantido para compatibilidade)
   */
  function buscarNomeTaskClickUp(taskId) {
    const task = buscarTaskClickUp(taskId);
    return task ? task.name : null;
  }

  /**
   * Busca as subtasks de uma task e extrai o Custom Field "🆔 ID GM Localização"
   * OTIMIZADO: Usa fetchAll para buscar todas as subtasks em paralelo
   * Retorna um Map: idGMLocalizacao -> { subtaskId, subtaskName, subtaskUrl, ... }
   */
  function buscarSubtasksComIdGM(taskId, taskJaBuscada) {
    // Usa a task já buscada se disponível, senão busca
    const task = taskJaBuscada || buscarTaskClickUp(taskId);
    if (!task || !task.subtasks || task.subtasks.length === 0) {
      console.log(`⚠️ Task ${taskId} não tem subtasks`);
      return new Map();
    }

    const mapaIdGM = new Map();
    const missingSubtasks = [];

    task.subtasks.forEach(subtask => {
      const cacheKey = `cu_subtask_${subtask.id}`;
      const cached = getCacheJson(cacheKey);
      if (cached && cached.idGM && cached.info) {
        mapaIdGM.set(cached.idGM, cached.info);
      } else {
        missingSubtasks.push(subtask);
      }
    });

    if (missingSubtasks.length === 0) {
      console.log(`📊 Total subtasks com ID GM: ${mapaIdGM.size} (cache)`);
      return mapaIdGM;
    }

    console.log(`📋 Buscando ${missingSubtasks.length} subtasks em paralelo...`);

    // Prepara todas as requisições para buscar em paralelo
    const clickUpToken = getClickUpToken_();
    const requests = missingSubtasks.map(subtask => ({
      url: `https://api.clickup.com/api/v2/task/${subtask.id}`,
      method: "GET",
      headers: {
        "Authorization": clickUpToken,
        "Content-Type": "application/json"
      },
      muteHttpExceptions: true
    }));

    try {
      // Busca todas as subtasks em paralelo!
      const responses = UrlFetchApp.fetchAll(requests);

      responses.forEach((response) => {
        if (response.getResponseCode() !== 200) return;

        const subtaskCompleta = JSON.parse(response.getContentText());
        let idGM = null;

        if (subtaskCompleta.custom_fields && Array.isArray(subtaskCompleta.custom_fields)) {
          const campo = subtaskCompleta.custom_fields.find(cf =>
            cf.name && cf.name.includes("ID GM Localização")
          );

          if (campo && campo.value) {
            idGM = String(campo.value).trim();
          }
        }

        if (idGM) {
          // 1. Tenta extrair do custom field de notas fiscais
          let nfes = "";
          let fieldId = null;
          let needsUpdate = false;

          const campoNF = subtaskCompleta.custom_fields.find(cf =>
            cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
          );

          if (campoNF) {
            fieldId = campoNF.id;
            if (campoNF.value !== undefined && campoNF.value !== null) {
              nfes = String(campoNF.value).trim();
            }
          }

          // 2. Se não achou na custom field, busca nos CHECKLISTS
          if (!nfes && subtaskCompleta.checklists && Array.isArray(subtaskCompleta.checklists)) {
            console.log(`   🔎 Buscando NFes nos checklists de "${subtaskCompleta.name}"...`);
            const itensChecklist = [];
            subtaskCompleta.checklists.forEach(checklist => {
              if (checklist.items && Array.isArray(checklist.items)) {
                checklist.items.forEach(item => {
                  if (item.name) itensChecklist.push(item.name.trim());
                });
              }
            });
            if (itensChecklist.length > 0) {
              nfes = itensChecklist.join(", ");
              needsUpdate = true; // Precisa atualizar o custom field com o que leu do checklist
            }
          }

          const info = {
            subtaskId: subtaskCompleta.id,
            subtaskName: subtaskCompleta.name,
            subtaskUrl: `https://app.clickup.com/t/${subtaskCompleta.id}`,
            status: subtaskCompleta.status ? subtaskCompleta.status.status : "",
            nfes: nfes,
            fieldId: fieldId,
            needsUpdate: needsUpdate
          };
          mapaIdGM.set(idGM, info);
          setCacheJson(`cu_subtask_${subtaskCompleta.id}`, { idGM: idGM, info: info }, CACHE_TTL_SUBTASK_SECONDS);
          console.log(`   ✅ ${subtaskCompleta.name} -> ID GM: ${idGM} | NFe: ${nfes || "N/A"}`);
        }
      });
    } catch (e) {
      console.error(`❌ Erro ao buscar subtasks: ${e.message}`);
    }

    console.log(`📊 Total subtasks com ID GM: ${mapaIdGM.size}`);
    return mapaIdGM;
  }

  function coletarNfesReentrega(taskId, taskJaBuscada) {
    const task = taskJaBuscada || buscarTaskClickUp(taskId);
    if (!task || !Array.isArray(task.subtasks) || task.subtasks.length === 0) {
      return "";
    }
    const clickUpToken = getClickUpToken_();
    const requests = task.subtasks.map(subtask => ({
      url: `https://api.clickup.com/api/v2/task/${subtask.id}`,
      method: "GET",
      headers: {
        "Authorization": clickUpToken,
        "Content-Type": "application/json"
      },
      muteHttpExceptions: true
    }));

    const nfesList = [];

    try {
      const responses = UrlFetchApp.fetchAll(requests);
      responses.forEach((response) => {
        if (response.getResponseCode() !== 200) return;
        const subtaskCompleta = JSON.parse(response.getContentText());
        let nfes = "";
        let fieldId = null;
        let needsUpdate = false;

        if (subtaskCompleta.custom_fields && Array.isArray(subtaskCompleta.custom_fields)) {
          const campoNF = subtaskCompleta.custom_fields.find(cf =>
            cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
          );
          if (campoNF) {
            fieldId = campoNF.id;
            if (campoNF.value !== undefined && campoNF.value !== null) {
              nfes = String(campoNF.value).trim();
            }
          }
        }

        if (!nfes && subtaskCompleta.checklists && Array.isArray(subtaskCompleta.checklists)) {
          const itensChecklist = [];
          subtaskCompleta.checklists.forEach(checklist => {
            if (checklist.items && Array.isArray(checklist.items)) {
              checklist.items.forEach(item => {
                if (item.name) itensChecklist.push(item.name.trim());
              });
            }
          });
          if (itensChecklist.length > 0) {
            nfes = itensChecklist.join(", ");
            needsUpdate = true;
          }
        }

        if (nfes) {
          nfesList.push(nfes);
          if (needsUpdate && fieldId) {
            atualizarCampoPersonalizadoClickUp(subtaskCompleta.id, fieldId, nfes);
          }
        }
      });
    } catch (e) {
      console.error(`❌ Erro ao buscar NFes reentrega: ${e.message}`);
    }

    return nfesList.join(", ");
  }

  /**
   * Atualiza um campo personalizado no ClickUp
   */
  function atualizarCampoPersonalizadoClickUp(taskId, fieldId, valor) {
    if (!taskId || !fieldId) return false;

    const url = `https://api.clickup.com/api/v2/task/${taskId}/field/${fieldId}`;
    const options = {
      method: "POST", // ClickUp usa POST para atualizar valor de custom field específico
      payload: JSON.stringify({
        value: valor
      })
    };

    try {
      console.log(`🔄 Atualizando custom field no ClickUp (Task: ${taskId})...`);
      const response = cuFetch_("cu-field", url, options);
      const code = response.getResponseCode();

      if (code === 200 || code === 204) {
        markClickUpPatched();
        console.log(`   ✅ Campo personalizado atualizado com sucesso.`);
        return true;
      } else {
        console.warn(`   ⚠️ Erro ao atualizar campo: ${code} - ${response.getContentText()}`);
      }
    } catch (e) {
      console.error(`   ❌ Erro ao atualizar campo no ClickUp: ${e.message}`);
    }
    return false;
  }

  /**
   * Atualiza o status de uma subtask no ClickUp
   * @param {string} subtaskId - ID da subtask
   * @param {string} novoStatus - Nome do status ("Retorno" ou "Finalizado")
   */
  function atualizarStatusSubtask(subtaskId, novoStatus) {
    if (!subtaskId || !novoStatus) return false;

    const url = `https://api.clickup.com/api/v2/task/${subtaskId}`;
    const options = {
      method: "PUT",
      payload: JSON.stringify({
        status: novoStatus
      })
    };

    try {
      console.log(`🔄 Atualizando subtask ${subtaskId} para status "${novoStatus}"...`);
      const response = cuFetch_("cu-status", url, options);
      const code = response.getResponseCode();

      if (code === 200 || code === 204) {
        markClickUpPatched();
        console.log(`   ✅ Status atualizado para "${novoStatus}"`);
        return true;
      } else {
        console.warn(`   ⚠️ Erro ao atualizar status: ${code} - ${response.getContentText()}`);
      }
    } catch (e) {
      console.error(`   ❌ Erro ao atualizar status: ${e.message}`);
    }

    return false;
  }

  /**
   * Atualiza o status de múltiplas subtasks em paralelo (batch)
   * @param {Array} updates - Array de { subtaskId, novoStatus, flatItem }
   */
  function atualizarStatusEmBatch(updates) {
    if (!updates || updates.length === 0) return;

    try {
      const requests = updates.map(update => ({
        url: `https://api.clickup.com/api/v2/task/${update.subtaskId}`,
        method: "PUT",
        payload: JSON.stringify({ status: update.novoStatus })
      }));

      const responses = cuFetchBatch_("cu-status", requests);
      responses.forEach((response, idx) => {
        const update = updates[idx];
        const code = response.getResponseCode();
        if (code === 200 || code === 204) {
          markClickUpPatched();
          console.log(`   ✅ ${update.subtaskId} -> "${update.novoStatus}"`);
          update.flatItem["clickup.subtaskStatus"] = update.novoStatus;
          if (update.subtaskInfo) {
            update.subtaskInfo.status = update.novoStatus;
          }
        } else {
          console.warn(`   ⚠️ Erro ${update.subtaskId}: ${code}`);
        }
      });
    } catch (e) {
      console.error(`❌ Erro no batch de status: ${e.message}`);
    }
  }

  function atualizarStatusSubtasksSimples(updates) {
    if (!updates || updates.length === 0) return;
    const requests = updates.map(update => ({
      url: `https://api.clickup.com/api/v2/task/${update.subtaskId}`,
      method: "PUT",
      payload: JSON.stringify({ status: update.novoStatus })
    }));
    const responses = cuFetchBatch_("cu-status", requests);
    responses.forEach((response, idx) => {
      const update = updates[idx];
      const code = response.getResponseCode();
      if (code === 200 || code === 204) {
        markClickUpPatched();
        console.log(`   ✅ ${update.subtaskId} -> "${update.novoStatus}"`);
      } else {
        console.warn(`   ⚠️ Erro ${update.subtaskId}: ${code}`);
      }
    });
  }

  /**
   * Determina o status da subtask baseado nos dados do GreenMile
   * @param {object} dadosGM - Dados do GreenMile (flatItem)
   * @returns {string|null} - "Retorno", "Finalizado" ou null (não alterar)
   */
  function determinarStatusSubtask(dadosGM) {
    const deliveryStatus = String(dadosGM["stop.deliveryStatus"] || "").toUpperCase();
    const motivoDevolucao = dadosGM["stop.undeliverableCode.description"];
    const saida = dadosGM["stop.actualDeparture"];

    // Se tem motivo de devolução -> Retorno
    if (motivoDevolucao && String(motivoDevolucao).trim() !== "") {
      return "Retorno";
    }

    // Se o status indica devolução/cancelamento
    if (deliveryStatus.includes("UNDELIVERED") ||
      deliveryStatus.includes("CANCELED") ||
      deliveryStatus.includes("RETURNED")) {
      return "Retorno";
    }

    // Se tem data de saída e status indica entregue -> Finalizado
    if (saida && deliveryStatus.includes("DELIVERED")) {
      return "Finalizada";
    }

    // Se tem data de saída (finalizou a parada) -> Finalizado
    if (saida) {
      return "Finalizada";
    }

    return null; // Não alterar
  }

  function aplicarReentregaSubtasksForaGM(mapaSubtasks, gmClientIds) {
    const missing = [];
    mapaSubtasks.forEach((info, idGM) => {
      const idKey = String(idGM || "").trim();
      if (!idKey) return;
      if (!gmClientIds.has(idKey)) {
        missing.push(info);
      }
    });

    if (missing.length === 0) return false;

    console.log(`📌 Subtasks fora do GreenMile: ${missing.length} -> Reentrega`);

    const subtaskIds = missing.map(m => m.subtaskId).filter(Boolean);
    const detalhes = buscarSubtasksDetalhes(subtaskIds);

    detalhes.forEach(subtask => {
      if (!subtask || !subtask.id) return;
      const { fieldId, fieldValue } = extrairCampoNfe(subtask);
      const nfChecklist = extrairNfesChecklist(subtask);
      const mergedNfes = mergeNfes(fieldValue, nfChecklist);
      if (mergedNfes && fieldId && precisaAtualizarCampo(fieldValue, mergedNfes)) {
        atualizarCampoPersonalizadoClickUp(subtask.id, fieldId, mergedNfes);
      }
    });

    const updates = subtaskIds.map(id => ({ subtaskId: id, novoStatus: "Reentrega" }));
    atualizarStatusSubtasksSimples(updates);
    return true;
  }

  function aplicarReentregaSubtasksSemIdGM(task, mapaSubtasks) {
    if (!task || !Array.isArray(task.subtasks)) return false;
    const subtaskIdsComIdGM = new Set();
    mapaSubtasks.forEach(info => {
      if (info && info.subtaskId) subtaskIdsComIdGM.add(String(info.subtaskId));
    });

    const semIdGM = task.subtasks.filter(st => st && st.id && !subtaskIdsComIdGM.has(String(st.id)));
    if (semIdGM.length === 0) return false;

    console.log(`📌 Subtasks sem ID GM: ${semIdGM.length} -> Reentrega`);

    const detalhes = buscarSubtasksDetalhes(semIdGM.map(s => s.id));
    detalhes.forEach(subtask => {
      if (!subtask || !subtask.id) return;
      const { fieldId, fieldValue } = extrairCampoNfe(subtask);
      const nfChecklist = extrairNfesChecklist(subtask);
      const mergedNfes = mergeNfes(fieldValue, nfChecklist);
      if (mergedNfes && fieldId && precisaAtualizarCampo(fieldValue, mergedNfes)) {
        atualizarCampoPersonalizadoClickUp(subtask.id, fieldId, mergedNfes);
      }
    });

    const updates = semIdGM.map(st => ({ subtaskId: st.id, novoStatus: "Reentrega" }));
    atualizarStatusSubtasksSimples(updates);
    return true;
  }

  function buscarSubtasksDetalhes(subtaskIds) {
    if (!Array.isArray(subtaskIds) || subtaskIds.length === 0) return [];
    const clickUpToken = getClickUpToken_();
    const requests = subtaskIds.map(id => ({
      url: `https://api.clickup.com/api/v2/task/${id}`,
      method: "GET",
      headers: {
        "Authorization": clickUpToken,
        "Content-Type": "application/json"
      },
      muteHttpExceptions: true
    }));

    const result = [];
    try {
      const responses = UrlFetchApp.fetchAll(requests);
      responses.forEach(response => {
        if (response.getResponseCode() !== 200) return;
        try {
          result.push(JSON.parse(response.getContentText()));
        } catch (e) { }
      });
    } catch (e) {
      console.error(`❌ Erro ao buscar detalhes das subtasks: ${e.message}`);
    }
    return result;
  }

  function extrairCampoNfe(task) {
    let fieldId = null;
    let fieldValue = "";
    if (task && Array.isArray(task.custom_fields)) {
      const campoNF = task.custom_fields.find(cf =>
        cf.name && (/notas? fiscais/i.test(cf.name) || /nfe/i.test(cf.name))
      );
      if (campoNF) {
        fieldId = campoNF.id;
        if (campoNF.value !== undefined && campoNF.value !== null) {
          fieldValue = String(campoNF.value).trim();
        }
      }
    }
    return { fieldId, fieldValue };
  }

  function extrairNfesChecklist(task) {
    if (!task || !Array.isArray(task.checklists)) return "";
    const itens = [];
    task.checklists.forEach(checklist => {
      if (checklist.items && Array.isArray(checklist.items)) {
        checklist.items.forEach(item => {
          if (item.name) itens.push(item.name.trim());
        });
      }
    });
    return itens.join(", ");
  }

  function mergeNfes(valorAtual, valorChecklist) {
    const set = new Set();
    [valorAtual, valorChecklist].forEach(valor => {
      if (!valor) return;
      String(valor).split(",").forEach(parte => {
        const item = String(parte).trim();
        if (item) set.add(item);
      });
    });
    return Array.from(set).join(", ");
  }

  function precisaAtualizarCampo(valorAtual, valorNovo) {
    if (!valorNovo) return false;
    if (!valorAtual) return true;
    const atualSet = new Set(String(valorAtual).split(",").map(v => v.trim()).filter(Boolean));
    const novoSet = new Set(String(valorNovo).split(",").map(v => v.trim()).filter(Boolean));
    if (novoSet.size > atualSet.size) return true;
    for (const item of novoSet) {
      if (!atualSet.has(item)) return true;
    }
    return false;
  }

  function valoresParaObjetos(values) {
    let headers = values[0];
    let result = [];
    for (let i = 1; i < values.length; i++) {
      let obj = {};
      let hasData = false;
      for (let j = 0; j < headers.length; j++) {
        let hOriginal = String(headers[j]).trim();
        // Traduz de PT para EN (chave interna), ou mantém se já for EN
        let h = COLUNAS_EN[hOriginal] || hOriginal;
        if (h) {
          obj[h] = values[i][j];
          if (values[i][j] !== "") hasData = true;
        }
      }
      if (hasData && obj["route.key"]) result.push(obj);
    }
    return result;
  }

  function prepararRequest(token, rotaKey, colunasDesejadas) {
    // Remove 'clickup.taskUrl' para não enviar pro GM
    const colunasGM = colunasDesejadas.filter(c => !c.startsWith("clickup."));

    const criteriaUrlObj = { "filters": colunasGM, "viewType": "STOP", "firstResult": 0, "maxResults": 1000 };
    const payloadBody = {
      "criteriaChain": [{ "and": [{ "attr": "route.key", "eq": rotaKey, "matchMode": "EXACT" }] }],
      "sort": [{ "attr": "stop.plannedSequenceNum", "type": "ASC" }]
    };

    return {
      "url": `${GM_URL_BASE}?criteria=${encodeURIComponent(JSON.stringify(criteriaUrlObj))}`,
      "method": "POST",
      "contentType": "application/json;charset=UTF-8",
      "headers": { "Authorization": "Bearer " + token, "Greenmile-Module": "LIVE", "Accept": "application/json" },
      "payload": JSON.stringify(payloadBody),
      "muteHttpExceptions": true
    };
  }

  function rotaConcluidaPorSubtasks(mapa) {
    if (!mapa || mapa.size === 0) return false;
    for (const valor of mapa.values()) {
      const status = String(valor.status || "").toLowerCase();
      if (!STATUS_FINAL_KEYWORDS.some(kw => status.includes(kw))) {
        return false;
      }
    }
    return true;
  }

  function rotaComChegadasESaidasCompletas(registros, rotaKey) {
    if (!rotaKey) return false;
    const rotaTrim = String(rotaKey).trim();
    const registrosRota = (registros || []).filter(r => String(r["route.key"] || "").trim() === rotaTrim);
    if (registrosRota.length === 0) return false;
    return registrosRota.every(reg => valorTemHorario(reg["stop.actualArrival"]) && valorTemHorario(reg["stop.actualDeparture"]));
  }

  function valorTemHorario(valor) {
    if (valor === null || valor === undefined) return false;
    if (valor instanceof Date) return true;
    if (typeof valor === "number") return true;
    return String(valor).trim().length > 0;
  }

  function chegadaAtrasada(chegadaIso) {
    if (!chegadaIso) return false;
    const date = new Date(chegadaIso);
    if (Number.isNaN(date.getTime())) return false;
    return date.getTime() + DURACAO_MINUTOS_NO_CLIENTE * 60000 < Date.now();
  }

  function isStatusCritico(status) {
    if (!status) return false;
    const lower = String(status).toLowerCase();
    return lower.includes("critico") || lower.includes("crítico");
  }

  function isStatusFinal(status) {
    if (!status) return false;
    const lower = String(status).toLowerCase();
    return STATUS_FINAL_KEYWORDS.some(keyword => lower.includes(keyword));
  }

  function chegadaSemSaida(flatItem) {
    if (!flatItem) return false;
    return valorTemHorario(flatItem["stop.actualArrival"]) && !valorTemHorario(flatItem["stop.actualDeparture"]);
  }

  function obterTeamId(task) {
    const defaultTeamId = "9007070798";
    if (!task) return defaultTeamId;
    return task.team_id || (task.team && task.team.id) || defaultTeamId;
  }

  function iniciarTimerClickUp(taskId, teamId, descricao) {
    if (!taskId || !teamId) {
      console.warn(`🕒 Timer não iniciado: falta taskId ou teamId (taskId=${taskId}, teamId=${teamId})`);
      return false;
    }

    const cacheKey = `timer_${taskId}`;
    if (scriptCache.get(cacheKey)) {
      return true;
    }

    const payload = {
      task_id: taskId,
      start: Date.now(),
      billable: false
    };
    if (descricao) payload.description = descricao;

    const options = {
      method: "POST",
      payload: JSON.stringify(payload)
    };
    const url = `https://api.clickup.com/api/v2/team/${teamId}/time_entries/start`;
    try {
      console.log(`🕒 Iniciando cronômetro "No Cliente" para ${taskId}${descricao ? ` (${descricao})` : ""}...`);
      const response = cuFetch_("cu-timer", url, options);
      const code = response.getResponseCode();
      if (code === 200 || code === 201 || code === 204) {
        scriptCache.put(cacheKey, "1", CACHE_TTL_TIMER_SECONDS);
        console.log(`   ✅ Timer iniciado para ${taskId}`);
        return true;
      }
      console.warn(`   ⚠️ Falha ao iniciar timer ${taskId}: ${code} - ${response.getContentText()}`);
    } catch (e) {
      console.error(`   ❌ Erro ao iniciar timer ClickUp: ${e.message}`);
    }
    return false;
  }

  function atualizarStatusTaskClickUp(taskId, novoStatus) {
    if (!taskId || !novoStatus) return false;
    return atualizarTaskClickUp(taskId, { status: novoStatus }, `status "${novoStatus}"`);
  }

  function ajustarDatasNoCliente(taskId, chegadaIso) {
    if (!taskId) return false;
    const chegada = chegadaIso ? new Date(chegadaIso).getTime() : Date.now();
    const payload = {
      start_date: chegada,
      due_date: chegada + DURACAO_MINUTOS_NO_CLIENTE * 60000,
      start_date_time: true,
      due_date_time: true
    };
    return atualizarTaskClickUp(taskId, payload, "datas No Cliente");
  }

  function atualizarTaskClickUp(taskId, payload, label) {
    if (!taskId || !payload) return false;
    const url = `https://api.clickup.com/api/v2/task/${taskId}`;
    const options = {
      method: "PUT",
      payload: JSON.stringify(payload)
    };
    try {
      console.log(`🔄 Atualizando task ${taskId} (${label})...`);
      const response = cuFetch_("cu-task", url, options);
      const code = response.getResponseCode();
      if (code === 200 || code === 204) {
        markClickUpPatched();
        console.log(`   ✅ ${label} atualizado com sucesso`);
        return true;
      }
      console.warn(`   ⚠️ Erro ao atualizar task (${label}): ${code} - ${response.getContentText()}`);
    } catch (e) {
      console.error(`   ❌ Erro ao atualizar task (${label}): ${e.message}`);
    }
    return false;
  }

  function obterRotasFinalizadasCache() {
    const raw = PropertiesService.getScriptProperties().getProperty(PROPS_KEY_ROTAS_FINALIZADAS);
    if (!raw) return new Set();
    try {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr)) {
        return new Set(arr);
      }
    } catch (e) {
      console.warn(`Erro ao ler cache de rotas finalizadas: ${e.message}`);
    }
    return new Set();
  }

  function persistirRotasFinalizadasCache(rotasSet) {
    if (!rotasSet) return;
    const arr = Array.from(rotasSet);
    PropertiesService.getScriptProperties().setProperty(PROPS_KEY_ROTAS_FINALIZADAS, JSON.stringify(arr));
  }

  function registrarRotaFinalizada(routeKey, taskId) {
    if (!routeKey) return;
    const chave = String(routeKey).trim();
    if (!chave) return;
    const rotas = obterRotasFinalizadasCache();
    if (rotas.has(chave)) return;
    rotas.add(chave);
    persistirRotasFinalizadasCache(rotas);
    gravarLogRotaFinalizada(chave, taskId);
  }

  function gravarLogRotaFinalizada(routeKey, taskId) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = ss.getSheetByName(SHEET_NAME_LOG_FINALIZADOS);
    if (!logSheet) {
      logSheet = ss.insertSheet(SHEET_NAME_LOG_FINALIZADOS);
      logSheet.getRange(1, 1, 1, 4).setValues([["Rota", "Task", "Data/Hora", "Observação"]]);
      logSheet.getRange(1, 1, 1, 4).setFontWeight("bold");
    }
    logSheet.appendRow([routeKey, taskId || "", new Date(), "Finalizada"]);
  }

  function detectarMudancasEntradaSaida(flatItems, routeKey, mapa, logAlteracoes) {
    if (!Array.isArray(flatItems) || !routeKey) return false;
    let changed = false;
    flatItems.forEach(flatItem => {
      const stopKey = String(flatItem["stop.location.key"] || flatItem["stop.location.id"] || flatItem["stop.id"] || "").trim();
      if (!stopKey) return;
      const arrival = normalizarHorarioEntrada(flatItem["stop.actualArrival"]);
      const departure = normalizarHorarioEntrada(flatItem["stop.actualDeparture"]);
      const mapKey = `${routeKey}||${stopKey}`;
      const stored = mapa.get(mapKey);
      const storedArrival = stored ? stored.arrival : "";
      const storedDeparture = stored ? stored.departure : "";
      const entryChanged = !stored || arrival !== storedArrival || departure !== storedDeparture;
      if (entryChanged) {
        changed = true;
        logAlteracoes.push({ route: routeKey, stopKey: stopKey, arrival, departure });
      }
      mapa.set(mapKey, { route: routeKey, stopKey: stopKey, arrival, departure, lastUpdate: new Date() });
    });
    return changed;
  }

  function normalizarHorarioEntrada(valor) {
    if (valor === null || valor === undefined) return "";
    if (valor instanceof Date) return valor.toISOString();
    const texto = String(valor).trim();
    if (!texto) return "";
    const data = new Date(texto);
    return Number.isNaN(data.getTime()) ? texto : data.toISOString();
  }

  function lerMapaEntradasSaidas() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA);
    const mapa = new Map();
    if (!sheet) return mapa;
    const values = sheet.getDataRange().getValues();
    for (let i = 1; i < values.length; i++) {
      const [route, stopKey, arrival, departure, lastUpdate] = values[i];
      const trimmedRoute = String(route || "").trim();
      const trimmedStopKey = String(stopKey || "").trim();
      if (!trimmedRoute || !trimmedStopKey) continue;
      mapa.set(`${trimmedRoute}||${trimmedStopKey}`, {
        route: trimmedRoute,
        stopKey: trimmedStopKey,
        arrival: String(arrival || ""),
        departure: String(departure || ""),
        lastUpdate: lastUpdate instanceof Date ? lastUpdate : (lastUpdate ? new Date(lastUpdate) : new Date())
      });
    }
    return mapa;
  }

  function salvarMapaEntradasSaidas(mapa) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA);
    if (!sheet) {
      sheet = ss.insertSheet(SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA);
    } else {
      sheet.clear();
    }
    const header = [["Route", "StopKey", "Arrival", "Departure", "LastUpdate"]];
    sheet.getRange(1, 1, 1, 5).setValues(header);
    sheet.getRange(1, 1, 1, 5).setFontWeight("bold");
    if (!mapa || mapa.size === 0) return;
    const rows = [];
    mapa.forEach(value => {
      rows.push([
        value.route || "",
        value.stopKey || "",
        value.arrival || "",
        value.departure || "",
        value.lastUpdate || new Date()
      ]);
    });
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  }

  function gravarLogEntradaSaida(changes) {
    if (!changes || changes.length === 0) return;
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(SHEET_NAME_LOG_ENTRADA_SAIDA);
    if (!sheet) {
      sheet = ss.insertSheet(SHEET_NAME_LOG_ENTRADA_SAIDA);
      sheet.getRange(1, 1, 1, 5).setValues([["Timestamp", "Route", "StopKey", "Arrival", "Departure"]]);
      sheet.getRange(1, 1, 1, 5).setFontWeight("bold");
    }
    const rows = changes.map(change => [
      new Date(),
      change.route,
      change.stopKey,
      change.arrival,
      change.departure
    ]);
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  }

  function limparMapaParaRota(mapa, routeKey) {
    if (!mapa || !routeKey) return;
    const prefix = `${routeKey}||`;
    Array.from(mapa.keys()).forEach(key => {
      if (key.startsWith(prefix)) {
        mapa.delete(key);
      }
    });
  }

}

function limparCacheNotificacoes() {
  const cache = CacheService.getScriptCache();
  cache.remove("gm_token");

  const props = PropertiesService.getScriptProperties();
  const todasProps = props.getProperties();
  const snapshotKeys = Object.keys(todasProps).filter(key => key.startsWith(SNAPSHOT_KEY_PREFIX));
  snapshotKeys.forEach(key => props.deleteProperty(key));

  const tinhaRotas = Object.prototype.hasOwnProperty.call(todasProps, PROPS_KEY_ROTAS_FINALIZADAS);
  if (tinhaRotas) {
    props.deleteProperty(PROPS_KEY_ROTAS_FINALIZADAS);
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  [SHEET_NAME_SNAPSHOT_ENTRADA_SAIDA, SHEET_NAME_LOG_ENTRADA_SAIDA].forEach(name => {
    const sheet = ss.getSheetByName(name);
    if (sheet) sheet.clear();
  });

  console.log(`⚡ Cache limpo: ${snapshotKeys.length} snapshot(s) removido(s), rotas finalizadas reset=${tinhaRotas}`);

  return {
    snapshotKeysRemoved: snapshotKeys.length,
    rotasFinalizadasReset: tinhaRotas,
    timestamp: new Date().toISOString()
  };
}

function parseNumeroSeguro(valor) {
  if (valor === null || valor === undefined) return 0;
  if (typeof valor === "number") return valor;
  const normalizado = String(valor).replace(/[^\d,.-]/g, "").replace(/\.(?=.*\.)/g, "").replace(",", ".");
  const parsed = parseFloat(normalizado);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function flattenObject(ob, prefix) {
  let toReturn = {};
  for (let i in ob) {
    if (!Object.prototype.hasOwnProperty.call(ob, i)) continue;
    let key = prefix ? `${prefix}.${i}` : i;
    if (typeof ob[i] === "object" && ob[i] !== null) {
      if (ob[i] instanceof Date) toReturn[key] = ob[i].toISOString();
      else {
        let f = flattenObject(ob[i], key);
        for (let x in f) {
          if (Object.prototype.hasOwnProperty.call(f, x)) {
            toReturn[x] = f[x];
          }
        }
      }
    } else {
      toReturn[key] = ob[i];
    }
  }
  return toReturn;
}

// ==============================================================================
// 5. MONITORAMENTO (VIGIA)
// ==============================================================================

/**
 * Agenda o Vigia para rodar a cada 5 minutos.
 * O Vigia verifica se os loops principais (runnerMain ou executarLoopListaClickUp)
 * estão ativos e rodando. Se não, ele os reinicia.
 */
function configurarVigia() {
  pararVigia(); // Limpa anterior para evitar duplicidade
  ScriptApp.newTrigger("monitorarStatusAgendamento")
    .timeBased()
    .everyMinutes(5)
    .create();
  console.log("👮 Vigia ativado (verificação a cada 5min).");
  SpreadsheetApp.getActiveSpreadsheet().toast("Vigia ativado com sucesso (5min).", "GreenMile");
}

function pararVigia() {
  const handler = "monitorarStatusAgendamento";
  const triggers = ScriptApp.getProjectTriggers().filter(t => t.getHandlerFunction() === handler);
  triggers.forEach(t => ScriptApp.deleteTrigger(t));
  console.log("👮 Vigia desativado.");
  SpreadsheetApp.getActiveSpreadsheet().toast("Vigia desativado.", "GreenMile");
}

function monitorarStatusAgendamento() {
  console.log("👮 [Vigia] Iniciando verificação...");
  const scriptProps = PropertiesService.getScriptProperties();

  // Verifica Runner (runner.js)
  const runnerHandler = "runnerMain";
  const triggersRunner = ScriptApp.getProjectTriggers().filter(t => t.getHandlerFunction() === runnerHandler);

  if (triggersRunner.length === 0) {
    console.warn("👮 [Vigia] Runner NÃO encontrado (sem triggers). Tentando iniciar...");
    try {
      runnerMain(); // Executa manualmente; ele deve criar o próprio trigger
      console.log("👮 [Vigia] Runner reiniciado com sucesso.");
    } catch (e) {
      console.error(`👮 [Vigia] Falha ao reiniciar Runner: ${e.message}`);
    }
  } else {
    // Verifica se travou (last summary muito antigo?)
    const lastSummaryRaw = scriptProps.getProperty("WF_LAST_RUN_SUMMARY");
    if (lastSummaryRaw) {
      try {
        const summary = JSON.parse(lastSummaryRaw);
        const lastRunTs = summary.endTs || summary.startTs;
        const now = Date.now();
        // Se a última execução foi há mais de 20 minutos (sendo que roda a cada 45s/1min), algo travou.
        if (now - lastRunTs > 20 * 60 * 1000) {
          console.warn("👮 [Vigia] Runner parece travado (última execução > 20min). Reiniciando...");
          // Mata triggers antigos e relança
          triggersRunner.forEach(t => ScriptApp.deleteTrigger(t));
          runnerMain(); // Relança execução manual que vai criar novo trigger
        } else {
          console.log("👮 [Vigia] Runner parece saudável (execução recente).");
        }
      } catch (e) {
        console.warn("👮 [Vigia] Erro ao ler summary do Runner. Ignorando.");
      }
    } else {
      console.log("👮 [Vigia] Runner ativo, mas sem sumário recente. Monitorando.");
    }
  }
}

/**
 * Busca uma task específica no ClickUp pelo ID.
 */
function buscarTaskClickUp(taskId) {
  if (!taskId) return null;
  const clickUpToken = getClickUpToken_();
  const url = `https://api.clickup.com/api/v2/task/${taskId}?include_subtasks=true`;
  const options = {
    method: "GET",
    headers: {
      "Authorization": clickUpToken,
      "Content-Type": "application/json"
    },
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    if (response.getResponseCode() === 200) {
      return JSON.parse(response.getContentText());
    } else {
      console.warn(`[WF] buscarTaskClickUp (${taskId}) falhou: HTTP ${response.getResponseCode()}`);
    }
  } catch (e) {
    console.warn(`[WF] buscarTaskClickUp (${taskId}) erro: ${e.message}`);
  }
  return null;
}
