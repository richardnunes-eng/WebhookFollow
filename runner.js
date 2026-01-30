// Runner & telemetry constants
const RUNNER_LOCK_TIMEOUT_MS = 10000;
const RUNNER_INTERVAL_MS = 45 * 1000;
const RUNNER_ERROR_INTERVAL_MS = 90 * 1000;
const PROPERTY_FINGERPRINT_PREFIX = "WF_ROUTE_FP:";
const PROPERTY_LAST_SUMMARY = "WF_LAST_RUN_SUMMARY";
const PROPERTY_GM_TOKEN_VALUE = "WF_GM_TOKEN";
const PROPERTY_GM_TOKEN_EXPIRES = "WF_GM_TOKEN_EXPIRES";
const PROPERTY_CLICKUP_CURSOR = "WF_CLICKUP_CURSOR";
const PROPERTY_MONITORED_ROUTES = "WF_MONITORED_ROUTES";
const PROPERTY_RUNNER_ERROR_STREAK = "WF_RUNNER_ERROR_STREAK";
const PROPERTY_BATCH_DATA_VERSION = "WF_BATCH_DATA_VERSION";
const PROPERTY_WEBHOOK_HEARTBEAT = "SITE_WEBHOOK_HEARTBEAT";
const PROPERTY_MODE = "MODE";
const PROPERTY_NEXT_RUN_TS = "WF_NEXT_RUN_TS";
const MAX_LOG_BODY_LENGTH = 2000;
const GM_LOGIN_URL = "https://3coracoes.greenmile.com/login";
const RUNNER_MIN_DELAY_MS = 60000; // Apps Script minimum is 1 minute

const RUNNER_OPERATIONAL_WINDOW = { startHour: 6, endHour: 19 };
const RUNNER_DURACAO_MINUTOS_NO_CLIENTE = 30;
const RUNNER_REENTREGA_STATUS = "Reentrega";

const MODE_BATCH_WEBHOOK = "BATCH_WEBHOOK";
const MODE_LEGACY = "LEGACY";
const DEFAULT_MODE = MODE_BATCH_WEBHOOK;
const GM_BATCH_SIZE = 50;
const SITE_WEBHOOK_URL_KEY = "SITE_WEBHOOK_URL";
const SITE_WEBHOOK_SECRET_KEY = "SITE_WEBHOOK_SECRET";
const SITE_WEBHOOK_MODE_KEY = "SITE_WEBHOOK_MODE";
const SITE_WEBHOOK_MODE_DELTA = "delta";
const SITE_WEBHOOK_MODE_PULL = "pull";
const VOLATILE_FIELD_PATTERNS = ["lastupdated", "updated", "created", "timestamp", "now"];
const RUNNER_STATUS_FINAL_KEYWORDS = ["finalizada", "done", "complete", "entregue", "retorno", "delivered"];

// ============================================================================
// DASHBOARD V2 CACHE CONSTANTS
// ============================================================================
const PROPERTY_DASH_V2_CACHE_PREFIX = "WF_DASH_V2_CACHE_";
const PROPERTY_DASH_V2_CACHE_INDEX = "WF_DASH_V2_CACHE_IDX";
const DASH_V2_CHUNK_SIZE = 8000; // Safe limit per property (~9KB max)

// Known custom field IDs for precise extraction (populate with actual IDs if needed)
const CF_IDS = {
  PLANO: null,
  MOTORISTA: null,
  PLACA: null,
  UNIDADE: null,
  VEICULO: null,
  MODALIDADE: null,
  REGIAO: null,
  DATA_SAIDA: null,
  QTD_ENTREGAS: null,
  ULTIMA_LOCALIZACAO: null,
  PROGRESSO: null,
  OCORRENCIAS: null,
  SINISTRO: null,
  PERNOITE: null
};

// Field name patterns for fallback matching (most specific first)
const CF_PATTERNS = {
  PLANO: ["🤖 plano", "plano"],
  MOTORISTA: ["🤖 motorista", "motorista"],
  PLACA: ["🚗 placa", "placa"],
  UNIDADE: ["📍 unidade", "unidade"],
  VEICULO: ["🚚 veículo", "🚛 veiculo", "veículo", "veiculo"],
  MODALIDADE: ["📋 modalidade", "modalidade"],
  REGIAO: ["🗺️ região de entrega", "🗺️ regiao", "região", "regiao"],
  DATA_SAIDA: ["📅 data de saída", "data de saída", "data de saida"],
  QTD_ENTREGAS: ["🚚 qtd. de entregas", "🤖 entregas"],
  ULTIMA_LOCALIZACAO: ["📍 localização da ultima", "última localização", "localização"],
  PROGRESSO: ["progresso de entregas automatic_progress", "progresso de entregas"],
  OCORRENCIAS: ["🔴 ocorrências", "ocorrências", "ocorrencia"],
  SINISTRO: ["sinistro"],
  PERNOITE: ["pernoite"]
};



const DEFAULT_CLICKUP_TOKEN = typeof CLICKUP_TOKEN === "undefined" ? "" : CLICKUP_TOKEN;
const DEFAULT_GM_USERNAME = typeof GM_USERNAME === "undefined" ? "" : GM_USERNAME;
const DEFAULT_GM_PASSWORD = typeof GM_PASSWORD === "undefined" ? "" : GM_PASSWORD;
let CURRENT_TELEMETRY = null;

function runnerMain() {
  const telemetry = createTelemetry();
  CURRENT_TELEMETRY = telemetry;
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(RUNNER_LOCK_TIMEOUT_MS)) {
    telemetry.status = "busy";
    telemetry.endTs = Date.now();
    telemetry.durationMs = telemetry.endTs - telemetry.startTs;
    persistTelemetrySummary(telemetry);
    console.log(`[WF][${telemetry.execId}] runner busy; another execution is running.`);
    rescheduleRunner_(RUNNER_INTERVAL_MS);
    CURRENT_TELEMETRY = null;
    return;
  }

  let delay = RUNNER_INTERVAL_MS;
  try {
    const scriptProps = PropertiesService.getScriptProperties();
    const mode = determineRunnerMode(scriptProps);
    console.log(`[WF][${telemetry.execId}] runner mode=${mode}`);
    if (mode === MODE_LEGACY) {
      processRunnerTasks(scriptProps, telemetry);
    } else {
      processBatchRunnerTasks(scriptProps, telemetry);
    }
    telemetry.status = "ok";
  } catch (error) {
    telemetry.status = "error";
    telemetry.error = error.message;
    delay = RUNNER_ERROR_INTERVAL_MS;
    console.error(`[WF][${telemetry.execId}] runner failed: ${error.message}`);
  } finally {
    telemetry.endTs = Date.now();
    telemetry.durationMs = telemetry.endTs - telemetry.startTs;
    persistTelemetrySummary(telemetry);
    handleRunnerHealth(telemetry.status);
    lock.releaseLock();
    rescheduleRunner_(delay);
    CURRENT_TELEMETRY = null;
  }
}

function processRunnerTasks(scriptProps, telemetry) {
  if (!LOOP_CLICKUP_LIST_ID) {
    throw new Error("LOOP_CLICKUP_LIST_ID não configurado.");
  }

  const tasks = buscarTasksAtivasDaLista(LOOP_CLICKUP_LIST_ID);
  telemetry.counters.plansTotal = tasks.length;
  telemetry.counters.plansChecked = 0;

  if (tasks.length > 0 && typeof prefetchTasksClickUpBatch === "function") {
    prefetchTasksClickUpBatch(tasks);
  }

  tasks.forEach((task) => {
    if (!task || !task.id) return;
    try {
      const routeKey = extractRouteKey(task) || "";
      reconciliarStatusTaskPrincipal(task.id, null, routeKey);
    } catch (error) {
      console.error(`[WF][${telemetry.execId}] reconcile ${task.id} falhou: ${error.message}`);
    }
  });

  const cursor = loadClickUpCursor(scriptProps);
  let maxCreatedAt = cursor.lastCreatedAt || 0;
  const storedRoutes = loadMonitoredRoutes(scriptProps);
  const updatedRoutes = {};

  tasks.forEach((task) => {
    if (!task || !task.id) return;
    const routeKey = extractRouteKey(task);
    if (!routeKey) return;
    updatedRoutes[task.id] = {
      routeKey: routeKey,
      name: task.name,
      id: task.id
    };
    if (!storedRoutes[task.id]) {
      console.log(`[WF] novo plano detectado: ${routeKey} (${task.id})`);
    }
    const createdAt = Number(task.date_created) || 0;
    if (createdAt > maxCreatedAt) {
      maxCreatedAt = createdAt;
    }
  });

  saveMonitoredRoutes(scriptProps, updatedRoutes);
  saveClickUpCursor(scriptProps, { lastCreatedAt: maxCreatedAt });

  tasks.forEach((task) => {
    if (!task || !task.id) return;
    telemetry.counters.plansChecked += 1;
    try {
      sincronizarGreenMileStable(task.id, true);
    } catch (error) {
      console.error(`[WF][${telemetry.execId}] task ${task.id} falhou: ${error.message}`);
      incrementTelemetryCounter("cuErrors");
    }
  });

  console.log(`[WF][${telemetry.execId}] processed ${telemetry.counters.plansChecked} planos; monitorando ${Object.keys(updatedRoutes).length}.`);
}

function determineRunnerMode(scriptProps) {
  const rawMode = scriptProps ? scriptProps.getProperty(PROPERTY_MODE) : null;
  if (!rawMode) return DEFAULT_MODE;
  const normalized = String(rawMode).trim().toUpperCase();
  return normalized === MODE_LEGACY ? MODE_LEGACY : MODE_BATCH_WEBHOOK;
}

function processBatchRunnerTasks(scriptProps, telemetry) {
  if (!LOOP_CLICKUP_LIST_ID) {
    throw new Error("LOOP_CLICKUP_LIST_ID não configurado.");
  }

  const tasks = buscarTasksAtivasDaLista(LOOP_CLICKUP_LIST_ID);
  telemetry.counters.plansTotal = tasks.length;
  telemetry.counters.plansChecked = tasks.length;
  const routeMap = buildRouteMapFromTasks(tasks);
  const routeKeys = Array.from(routeMap.keys());
  telemetry.counters.checkedRoutes = routeKeys.length;

  if (tasks.length > 0 && typeof prefetchTasksClickUpBatch === "function") {
    prefetchTasksClickUpBatch(tasks);
  }

  console.log(`[WF][${telemetry.execId}] batch checking ${routeKeys.length} rota(s)`);

  if (routeKeys.length === 0) {
    maybeSendHeartbeat(scriptProps, telemetry);
    return;
  }

  const snapshots = fetchGreenMileSnapshots(routeKeys);
  telemetry.counters.batchFetchCount = routeKeys.length;
  const changedCandidates = [];

  routeKeys.forEach((routeKey) => {
    const snapshot = snapshots[routeKey];
    if (!snapshot || snapshot.error) {
      console.warn(`[WF][${telemetry.execId}] rota ${routeKey} falhou ao baixar dados: ${snapshot ? snapshot.error : "sem dados"}`);
      return;
    }
    const previousFingerprint = getRouteFingerprint(routeKey);
    if (!previousFingerprint || previousFingerprint !== snapshot.fingerprint) {
      changedCandidates.push(routeKey);
    }
  });

  telemetry.counters.changedRoutes = changedCandidates.length;

  // Identify Pure Reentrega Tasks (No Route Key or Non-610)
  const pureReentregaTasks = [];
  tasks.forEach((task) => {
    if (!task || !task.id) return;
    const rKey = extractRouteKey(task);
    if (!rKey || !String(rKey).startsWith("610")) {
      pureReentregaTasks.push(task);
    }
  });

  if (pureReentregaTasks.length > 0) {
    console.log(`[WF][${telemetry.execId}] processando ${pureReentregaTasks.length} rotas de reentrega pura...`);
    pureReentregaTasks.forEach(task => {
      try {
        // Force full sync for pure reentrega to apply status/NFes logic
        sincronizarGreenMileStable(task.id, true);
        incrementTelemetryCounter("clickUpPatched"); // Count as patched
      } catch (e) {
        console.error(`[WF][${telemetry.execId}] erro ao sincronizar reentrega pura ${task.id}: ${e.message}`);
        incrementTelemetryCounter("cuErrors");
      }
    });
  }

  tasks.forEach((task) => {
    if (!task || !task.id) return;
    try {
      const routeKey = extractRouteKey(task) || "";
      const snapshot = routeKey ? snapshots[routeKey] : null;
      reconciliarStatusTaskPrincipal(task.id, snapshot || null, routeKey);
    } catch (error) {
      console.error(`[WF][${telemetry.execId}] reconcile ${task.id} falhou: ${error.message}`);
    }
  });

  if (changedCandidates.length === 0) {
    console.log(`[WF][${telemetry.execId}] nenhuma mudança detectada (GM).`);
    // Build and persist DashboardV2 cache even on heartbeat
    try {
      const dashModel = buildDashModel(tasks, routeMap, snapshots, scriptProps, telemetry);
      persistDashV2Cache(dashModel, scriptProps);
    } catch (dashErr) {
      console.error(`[WF] Failed to build dashModel: ${dashErr.message}`);
    }
    maybeSendHeartbeat(scriptProps, telemetry);
    return;
  }

  const patchedRoutes = [];
  const patchErrors = [];

  changedCandidates.forEach((routeKey) => {
    const routeInfo = routeMap.get(routeKey);
    const taskId = routeInfo ? routeInfo.taskId : null;
    try {
      sincronizarGreenMileStable(taskId, true);
      patchedRoutes.push(routeKey);
    } catch (error) {
      patchErrors.push({ routeKey, error: error.message });
      console.error(`[WF][${telemetry.execId}] erro ao sincronizar ${routeKey}: ${error.message}`);
      incrementTelemetryCounter("cuErrors");
    }
  });

  telemetry.counters.patchedRoutes = patchedRoutes.length;

  if (patchedRoutes.length === 0) {
    console.warn(`[WF][${telemetry.execId}] mudanças detectadas, mas nenhuma rota patchada.`);
    return;
  }

  // Build and persist DashboardV2 cache on successful sync
  try {
    const dashModel = buildDashModel(tasks, routeMap, snapshots, scriptProps, telemetry);
    persistDashV2Cache(dashModel, scriptProps);
  } catch (dashErr) {
    console.error(`[WF] Failed to build dashModel: ${dashErr.message}`);
  }

  const dataVer = incrementDataVersion(scriptProps);
  const requestedMode = String(getConfiguredValue(SITE_WEBHOOK_MODE_KEY, SITE_WEBHOOK_MODE_DELTA) || SITE_WEBHOOK_MODE_DELTA).trim().toLowerCase();
  const payloadMode = requestedMode === SITE_WEBHOOK_MODE_PULL ? SITE_WEBHOOK_MODE_PULL : SITE_WEBHOOK_MODE_DELTA;
  const payload = {
    source: "WebhookFollow",
    execId: telemetry.execId,
    dataVer: String(dataVer),
    ts: Date.now(),
    counters: {
      checked: routeKeys.length,
      changed: patchedRoutes.length,
      errors: patchErrors.length
    },
    mode: payloadMode
  };

  if (payloadMode === SITE_WEBHOOK_MODE_DELTA) {
    payload.changedRouteIds = patchedRoutes;
  }

  const sent = sitePublishUpdate_(payload);
  if (sent) {
    telemetry.counters.webhookSent += 1;
  }
}

function buildRouteMapFromTasks(tasks) {
  const map = new Map();
  (tasks || []).forEach((task) => {
    if (!task || !task.id) return;
    const routeKey = extractRouteKey(task);
    if (!routeKey) return;
    const trimmed = String(routeKey).trim();
    if (!trimmed) return;
    const existing = map.get(trimmed) || { taskIds: [] };
    existing.taskIds.push(task.id);
    existing.taskId = existing.taskId || task.id;
    existing.taskName = task.name || existing.taskName;
    existing.taskUrl = existing.taskUrl || task.url || `https://app.clickup.com/t/${task.id}`;
    map.set(trimmed, existing);
  });
  return map;
}


function createTelemetry() {
  const startTs = Date.now();
  return {
    execId: `wf-${startTs}-${Math.random().toString(36).slice(2, 8)}`,
    startTs: startTs,
    endTs: null,
    durationMs: null,
    status: "starting",
    counters: {
      plansTotal: 0,
      plansChecked: 0,
      plansChanged: 0,
      clickUpPatched: 0,
      gmErrors: 0,
      cuErrors: 0,
      batchFetchCount: 0,
      checkedRoutes: 0,
      changedRoutes: 0,
      patchedRoutes: 0,
      webhookSent: 0
    }
  };
}

function fetchGreenMileSnapshots(routeKeys) {
  const result = Object.create(null);
  if (!Array.isArray(routeKeys) || routeKeys.length === 0) {
    return result;
  }

  const token = getGreenMileToken_();
  for (let i = 0; i < routeKeys.length; i += GM_BATCH_SIZE) {
    const chunk = routeKeys.slice(i, i + GM_BATCH_SIZE);
    const requests = chunk.map((routeKey) => buildGreenMileRequest(token, routeKey));
    const responses = gmFetchBatch_("gm-batch", requests);
    chunk.forEach((routeKey, index) => {
      const response = responses[index];
      if (response.getResponseCode() !== 200) {
        incrementTelemetryCounter("gmErrors");
        result[routeKey] = {
          error: `HTTP ${response.getResponseCode()}`,
          status: response.getResponseCode(),
          body: sanitizeBody(response.getContentText())
        };
        return;
      }
      try {
        const json = JSON.parse(response.getContentText());
        const items = json.content || json.rows || json.items || json;
        const normalized = normalizeRouteSnapshot(routeKey, Array.isArray(items) ? items : []);
        const fingerprint = computeFingerprint(routeKey, normalized);
        result[routeKey] = { fingerprint, rows: normalized.length, items: normalized };
      } catch (error) {
        incrementTelemetryCounter("gmErrors");
        console.error(`[WF] falha ao processar resposta GM para ${routeKey}: ${error.message}`);
        result[routeKey] = { error: error.message };
      }
    });
    if (i + GM_BATCH_SIZE < routeKeys.length) {
      Utilities.sleep(100);
    }
  }
  return result;
}

function normalizeRouteSnapshot(routeKey, items) {
  return (items || []).map((item) => {
    const flat = flattenObject(Object.assign({}, item), "");
    if (!flat["route.key"]) {
      flat["route.key"] = routeKey;
    }
    const ordersInfo = flat["stop.ordersInfo"];
    if (ordersInfo) {
      flat["stop.orders.number"] = String(ordersInfo).replace(/[\[\]"']/g, "");
    }
    flat["stop.plannedSize3"] = parseNumeroSeguro(
      flat["stop.plannedSize3"] ?? flat["stop.plannedSize3.value"] ?? flat["stop.plannedSize3.amount"]
    );
    flat["stop.plannedSize1"] = parseNumeroSeguro(flat["stop.plannedSize1"] || 0);
    const sequence = Number.parseInt(flat["stop.plannedSequenceNum"] || 0, 10);
    flat["stop.plannedSequenceNum"] = Number.isNaN(sequence) ? 0 : sequence;
    delete flat["stop.ordersInfo"];
    return removeVolatileKeys(flat);
  });
}

function buildGreenMileRequest(token, routeKey) {
  const allowedFields = (COLUNAS_GM || []).filter((field) => !field.startsWith("clickup."));
  const criteriaUrlObj = { filters: allowedFields, viewType: "STOP", firstResult: 0, maxResults: 1000 };
  const payloadBody = {
    criteriaChain: [{ and: [{ attr: "route.key", eq: routeKey, matchMode: "EXACT" }] }],
    sort: [{ attr: "stop.plannedSequenceNum", type: "ASC" }]
  };
  return {
    url: `${GM_URL_BASE}?criteria=${encodeURIComponent(JSON.stringify(criteriaUrlObj))}`,
    method: "POST",
    contentType: "application/json;charset=UTF-8",
    headers: {
      Authorization: `Bearer ${token}`,
      "Greenmile-Module": "LIVE",
      Accept: "application/json"
    },
    payload: JSON.stringify(payloadBody),
    muteHttpExceptions: true
  };
}

function removeVolatileKeys(entry) {
  const sanitized = {};
  Object.keys(entry || {}).forEach((key) => {
    const lower = key.toLowerCase();
    if (key.startsWith("clickup.")) return;
    if (VOLATILE_FIELD_PATTERNS.some((pattern) => lower.includes(pattern))) return;
    sanitized[key] = entry[key];
  });
  return sanitized;
}

function maybeSendHeartbeat(scriptProps, telemetry) {
  if (!isWebhookHeartbeatEnabled()) return;
  const dataVer = getCurrentDataVersion(scriptProps);
  const payload = {
    source: "WebhookFollow",
    execId: telemetry.execId,
    dataVer: String(dataVer),
    ts: Date.now(),
    mode: "heartbeat",
    counters: {
      checked: telemetry.counters.checkedRoutes || 0,
      changed: 0,
      errors: 0
    }
  };
  if (sitePublishUpdate_(payload)) {
    telemetry.counters.webhookSent += 1;
  }
}

function isWebhookHeartbeatEnabled() {
  const heartbeatFlag = getConfiguredValue(PROPERTY_WEBHOOK_HEARTBEAT, "false");
  return String(heartbeatFlag || "").trim().toLowerCase() === "true";
}

function incrementDataVersion(scriptProps) {
  const current = Number(scriptProps.getProperty(PROPERTY_BATCH_DATA_VERSION) || "0");
  const next = current + 1;
  scriptProps.setProperty(PROPERTY_BATCH_DATA_VERSION, String(next));
  return next;
}

function getCurrentDataVersion(scriptProps) {
  return Number(scriptProps.getProperty(PROPERTY_BATCH_DATA_VERSION) || "0");
}

function sitePublishUpdate_(payload) {
  const webhookUrl = getConfiguredValue(SITE_WEBHOOK_URL_KEY, "");
  if (!webhookUrl) {
    console.log("[WF] site webhook não configurado.");
    return false;
  }
  const body = JSON.stringify(payload);
  const options = {
    method: "post",
    contentType: "application/json",
    payload: body,
    muteHttpExceptions: true,
    headers: {}
  };
  const secret = getConfiguredValue(SITE_WEBHOOK_SECRET_KEY, "");
  if (secret) {
    options.headers["X-WF-Signature"] = buildSiteSignature(body, secret);
  }

  try {
    const response = UrlFetchApp.fetch(webhookUrl, options);
    logHttpResponse("site-webhook", webhookUrl, response, null);
    const code = response.getResponseCode();
    return code >= 200 && code < 300;
  } catch (error) {
    console.error(`[WF] falha ao enviar webhook para o site: ${error.message}`);
    return false;
  }
}

function buildSiteSignature(body, secret) {
  const digest = Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, body, secret);
  return digest.map((byte) => (`0${(byte & 0xff).toString(16)}`).slice(-2)).join("");
}

function persistTelemetrySummary(telemetry) {
  const counters = telemetry.counters || {};
  const summary = {
    execId: telemetry.execId,
    status: telemetry.status,
    startTs: telemetry.startTs,
    endTs: telemetry.endTs,
    durationMs: telemetry.durationMs,
    counters: counters,
    // Computed summary fields for easy monitoring
    clickupTasksFetched: counters.plansTotal || 0,
    gmRoutesFetched: counters.batchFetchCount || 0,
    changedRoutes: counters.changedRoutes || 0,
    webhookSent: counters.webhookSent || 0,
    errorsCount: (counters.gmErrors || 0) + (counters.cuErrors || 0)
  };

  const summaryJson = JSON.stringify(summary);
  PropertiesService.getScriptProperties().setProperty(PROPERTY_LAST_SUMMARY, summaryJson);

  // Log structured summary for easy scanning in execution logs
  Logger.log(`[WF] Run Summary: tasks=${summary.clickupTasksFetched}, routes=${summary.gmRoutesFetched}, changed=${summary.changedRoutes}, webhooks=${summary.webhookSent}, errors=${summary.errorsCount}, duration=${summary.durationMs}ms`);
  console.log(`[WF][${telemetry.execId}] summary: ${summaryJson}`);
}

function incrementTelemetryCounter(key, amount) {
  if (!CURRENT_TELEMETRY) return;
  const counters = CURRENT_TELEMETRY.counters;
  if (!Object.prototype.hasOwnProperty.call(counters, key)) {
    counters[key] = 0;
  }
  counters[key] += amount || 1;
}

function markClickUpPatched() {
  incrementTelemetryCounter("clickUpPatched");
}

function markPlanChanged() {
  incrementTelemetryCounter("plansChanged");
}

function rescheduleRunner_(delayMs) {
  const handler = "runnerMain";
  const scriptProps = PropertiesService.getScriptProperties();

  // Enforce minimum delay of 60s (Apps Script limitation)
  const desiredDelay = Math.max(delayMs || RUNNER_INTERVAL_MS, RUNNER_MIN_DELAY_MS);
  const actualDelay = resolveOperationalDelay(desiredDelay);
  const nextRunTs = Date.now() + actualDelay;

  // Check if we already have a sooner run scheduled
  const existingNextRun = Number(scriptProps.getProperty(PROPERTY_NEXT_RUN_TS) || "0");
  if (existingNextRun > Date.now() && existingNextRun <= nextRunTs) {
    console.log(`[WF] Runner already scheduled sooner (${new Date(existingNextRun).toISOString()}), skipping.`);
    return;
  }

  // Delete existing triggers
  const triggers = ScriptApp.getProjectTriggers().filter((trigger) => trigger.getHandlerFunction() === handler);
  triggers.forEach((trigger) => ScriptApp.deleteTrigger(trigger));

  // Create new trigger and save timestamp
  ScriptApp.newTrigger(handler).timeBased().after(actualDelay).create();
  scriptProps.setProperty(PROPERTY_NEXT_RUN_TS, String(nextRunTs));

  console.log(`[WF] Runner rescheduled for ${actualDelay}ms (next: ${new Date(nextRunTs).toISOString()}), removed ${triggers.length} old trigger(s).`);
}

function resolveOperationalDelay(desiredDelay) {
  const targetTime = new Date(Date.now() + desiredDelay);
  if (isWithinOperationalWindow(targetTime)) {
    return desiredDelay;
  }
  // Schedule for next operational window start (timezone-safe)
  const tz = Session.getScriptTimeZone();
  const nowDate = new Date();
  const nextRun = new Date();
  nextRun.setHours(RUNNER_OPERATIONAL_WINDOW.startHour, 0, 0, 0);
  if (nextRun.getTime() <= Date.now()) {
    nextRun.setDate(nextRun.getDate() + 1);
  }
  console.log(`[WF] Outside operational window. Next run at ${Utilities.formatDate(nextRun, tz, "yyyy-MM-dd HH:mm:ss z")}`);
  return nextRun.getTime() - Date.now();
}

function isWithinOperationalWindow(timestamp) {
  // Use script timezone for accurate hour calculation
  const tz = Session.getScriptTimeZone();
  const hourStr = Utilities.formatDate(timestamp, tz, "H");
  const hour = parseInt(hourStr, 10);
  return hour >= RUNNER_OPERATIONAL_WINDOW.startHour && hour < RUNNER_OPERATIONAL_WINDOW.endHour;
}

function loadMonitoredRoutes(scriptProps) {
  const raw = scriptProps.getProperty(PROPERTY_MONITORED_ROUTES);
  if (!raw) return {};
  try {
    return JSON.parse(raw) || {};
  } catch (error) {
    console.warn(`[WF] impossible to parse monitored routes: ${error.message}`);
  }
  return {};
}

function saveMonitoredRoutes(scriptProps, routes) {
  scriptProps.setProperty(PROPERTY_MONITORED_ROUTES, JSON.stringify(routes || {}));
}

function loadClickUpCursor(scriptProps) {
  const raw = scriptProps.getProperty(PROPERTY_CLICKUP_CURSOR);
  if (!raw) return { lastCreatedAt: 0 };
  try {
    return JSON.parse(raw);
  } catch (error) {
    console.warn(`[WF] cursor parse failed: ${error.message}`);
    return { lastCreatedAt: 0 };
  }
}

function saveClickUpCursor(scriptProps, cursor) {
  scriptProps.setProperty(PROPERTY_CLICKUP_CURSOR, JSON.stringify(cursor || { lastCreatedAt: 0 }));
}

function getConfiguredValue(key, fallback) {
  const value = PropertiesService.getScriptProperties().getProperty(key);
  if (value && value.toString().trim().length > 0) {
    return value.trim();
  }
  return fallback;
}

function getClickUpToken_() {
  const token = getConfiguredValue("CLICKUP_TOKEN", DEFAULT_CLICKUP_TOKEN);
  if (!token) throw new Error("ClickUp token não configurado.");
  return token;
}

function getGreenMileToken_() {
  const props = PropertiesService.getScriptProperties();
  const cached = props.getProperty(PROPERTY_GM_TOKEN_VALUE);
  const expires = Number(props.getProperty(PROPERTY_GM_TOKEN_EXPIRES)) || 0;
  if (cached && expires > Date.now()) {
    return cached;
  }

  const username = getConfiguredValue("GM_USERNAME", DEFAULT_GM_USERNAME);
  const password = getConfiguredValue("GM_PASSWORD", DEFAULT_GM_PASSWORD);
  if (!username || !password) {
    throw new Error("Credenciais GreenMile não configuradas.");
  }

  const payload = `j_username=${encodeURIComponent(username)}&j_password=${encodeURIComponent(password)}`;
  const response = gmFetch_("gm-auth", GM_LOGIN_URL, {
    method: "post",
    payload: payload,
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Greenmile-Module": "LIVE"
    }
  });

  const code = response.getResponseCode();
  if (code !== 200) {
    throw new Error(`GreenMile auth falhou (${code}).`);
  }

  const json = JSON.parse(response.getContentText());
  const token = json.analyticsToken?.access_token || json.access_token;
  const expiresIn = json.analyticsToken?.expires_in || json.expires_in || 3600;
  if (!token) {
    throw new Error("Ainda não foi possível extrair o token do GreenMile.");
  }

  const validUntil = Date.now() + (Number(expiresIn || 0) * 1000) - 30000;
  props.setProperty(PROPERTY_GM_TOKEN_VALUE, token);
  props.setProperty(PROPERTY_GM_TOKEN_EXPIRES, String(validUntil));

  return token;
}

function gmFetch_(where, url, options) {
  const opts = Object.assign({ muteHttpExceptions: true }, options || {});
  opts.headers = Object.assign({ Accept: "application/json", "User-Agent": "WebhookFollow/1.0" }, opts.headers || {});
  try {
    const response = UrlFetchApp.fetch(url, opts);
    logHttpResponse(where, url, response, "gmErrors");
    return response;
  } catch (error) {
    logHttpError(where, url, error);
    incrementTelemetryCounter("gmErrors");
    throw error;
  }
}

function gmFetchBatch_(where, requests) {
  if (!Array.isArray(requests)) {
    throw new Error("gmFetchBatch_ precisa de um array de requests.");
  }
  const sanitized = requests.map((request) => {
    const cloned = Object.assign({}, request);
    cloned.muteHttpExceptions = true;
    cloned.headers = Object.assign({ Accept: "application/json", "User-Agent": "WebhookFollow/1.0" }, cloned.headers || {});
    return cloned;
  });

  let responses;
  try {
    responses = UrlFetchApp.fetchAll(sanitized);
  } catch (batchError) {
    console.error(`[${where}] fetchAll batch failed: ${batchError.message}`);
    incrementTelemetryCounter("gmErrors", requests.length);
    // Return empty array-like responses to prevent caller crash
    return sanitized.map(() => ({
      getResponseCode: () => 500,
      getContentText: () => JSON.stringify({ error: batchError.message })
    }));
  }

  responses.forEach((response, index) => {
    const logUrl = sanitized[index].url;
    logHttpResponse(where, logUrl, response, "gmErrors");
  });
  return responses;
}

function cuFetch_(where, url, options) {
  const token = getClickUpToken_();
  const opts = Object.assign({}, options || {});
  opts.headers = Object.assign({ Authorization: token, "Content-Type": "application/json" }, opts.headers || {});
  opts.muteHttpExceptions = true;
  const maxAttempts = 3;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      const response = UrlFetchApp.fetch(url, opts);
      logHttpResponse(where, url, response, "cuErrors");
      if (response.getResponseCode() !== 429) {
        return response;
      }
      Utilities.sleep(1000 * Math.pow(2, attempt));
      attempt += 1;
    } catch (error) {
      logHttpError(where, url, error);
      incrementTelemetryCounter("cuErrors");
      throw error;
    }
  }
  const finalResponse = UrlFetchApp.fetch(url, opts);
  logHttpResponse(where, url, finalResponse, "cuErrors");
  return finalResponse;
}

function cuFetchBatch_(where, requests) {
  if (!Array.isArray(requests)) {
    throw new Error("cuFetchBatch_ precisa de um array de requests.");
  }
  const token = getClickUpToken_();
  const sanitized = requests.map((request) => {
    const cloned = Object.assign({}, request);
    cloned.muteHttpExceptions = true;
    cloned.headers = Object.assign({ Authorization: token, "Content-Type": "application/json" }, cloned.headers || {});
    return cloned;
  });

  let responses;
  try {
    responses = UrlFetchApp.fetchAll(sanitized);
  } catch (batchError) {
    console.error(`[${where}] fetchAll batch failed: ${batchError.message}`);
    incrementTelemetryCounter("cuErrors", requests.length);
    // Return empty array-like responses to prevent caller crash
    return sanitized.map(() => ({
      getResponseCode: () => 500,
      getContentText: () => JSON.stringify({ error: batchError.message })
    }));
  }

  responses.forEach((response, index) => {
    const logUrl = sanitized[index].url;
    logHttpResponse(where, logUrl, response, "cuErrors");
  });
  return responses;
}

function buscarTaskClickUpParaReconciliar(taskId) {
  if (!taskId) return null;
  const cache = CacheService.getScriptCache();
  const cacheKey = `cu_task_${taskId}`;
  try {
    const cached = cache.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }
  } catch (e) { }

  const url = `https://api.clickup.com/api/v2/task/${taskId}?include_subtasks=true`;
  const response = cuFetch_("cu-task", url, { method: "GET" });
  const code = response.getResponseCode();
  if (code !== 200 && code !== 204) {
    console.warn(`[WF] reconcile task ${taskId} HTTP ${code}`);
    return null;
  }
  try {
    const json = JSON.parse(response.getContentText());
    try { cache.put(cacheKey, JSON.stringify(json), 120); } catch (e) { }
    return json;
  } catch (e) {
    console.warn(`[WF] reconcile parse task ${taskId}: ${e.message}`);

    return null;
  }
}



function updateTaskChecklistProgress(taskId, subtasksDetalhes) {
  if (!taskId || !Array.isArray(subtasksDetalhes) || subtasksDetalhes.length === 0) return;

  // Filtra apenas subtasks "relevantes" (ex: entregas reais)
  // Se quiser incluir todas, basta remover o filtro ou ajustar.
  // Aqui assumimos que todas as subtasks com ID GM são entregas.
  const relevantSubtasks = subtasksDetalhes.filter(st => {
    // Opcional: filtrar apenas as que têm ID GM se quiser ignorar manuais
    // const idGM = extrairIdGM(st);
    // return !!idGM;
    return true;
  });

  if (relevantSubtasks.length === 0) return;

  const checklistName = "Progresso Automático";
  const checklist = ensureChecklistExists(taskId, checklistName);

  if (!checklist || !checklist.id) {
    console.warn(`[WF] Falha ao obter/criar checklist "${checklistName}" na task ${taskId}`);
    return;
  }

  // Mapeia itens existentes para não duplicar (busca por nome)
  const existingItemsMap = new Map();
  if (Array.isArray(checklist.items)) {
    checklist.items.forEach(item => existingItemsMap.set(item.name.trim(), item));
  }

  relevantSubtasks.forEach(subtask => {
    const itemName = subtask.name.trim(); // Nome do item = Nome da Subtask
    const statusText = subtask.status ? subtask.status.status : "";

    // É finalizado SE for status final E NÃO CONTIVER "retorno" ou "reentrega"
    const isFinished = isStatusFinal(statusText) &&
      !statusText.toLowerCase().includes("retorno") &&
      !statusText.toLowerCase().includes("reentrega");

    console.log(`[WF] Checklist Check: "${itemName}" | Status: "${statusText}" | Finished? ${isFinished}`);

    const existingItem = existingItemsMap.get(itemName);

    if (existingItem) {
      if (existingItem.resolved !== isFinished) {
        updateChecklistItem(checklist.id, existingItem.id, isFinished);
      }
    } else {
      createChecklistItem(checklist.id, itemName, isFinished);
    }
  });
}





function isStatusFinal(status) {
  if (!status) return false;
  const lower = status.toLowerCase();
  return RUNNER_STATUS_FINAL_KEYWORDS.some(k => lower.includes(k));
}

function reconciliarStatusTaskPrincipal(taskId, snapshot, routeKey) {
  if (!taskId) return false;
  const task = buscarTaskClickUpParaReconciliar(taskId);
  if (!task) return false;
  const subtasks = Array.isArray(task.subtasks) ? task.subtasks : [];
  if (subtasks.length === 0) return false;

  const statusAtual = task.status ? task.status.status : "";
  const lowerAtual = String(statusAtual || "").toLowerCase();
  let temNoCliente = false;
  let temCritico = false;
  let todosFinalizados = true;

  subtasks.forEach((sub) => {
    const st = String((sub.status && sub.status.status) || sub.status || "").toLowerCase();
    if (st.includes("no cliente")) temNoCliente = true;
    if (st.includes("critico") || st.includes("crítico")) temCritico = true;
    if (!RUNNER_STATUS_FINAL_KEYWORDS.some((kw) => st.includes(kw))) {
      todosFinalizados = false;
    }
  });

  let statusEsperado = null;
  const gmRows = snapshot && Array.isArray(snapshot.items) ? snapshot.items : [];
  if (snapshot && snapshot.error) {
    return false;
  }
  const rotaNaoGM = !routeKey || !String(routeKey).startsWith("610");
  if (rotaNaoGM) {
    // aplicarReentregaSemGreenMile(task); // DISABLED: Causing all subtasks to become Reentrega
    // statusEsperado = RUNNER_REENTREGA_STATUS; // check if we should even set the main task status?
    // User complaint suggests we should STOP touching these tasks entirely.
    return false;
  } else if (snapshot && gmRows.length === 0) {
    // Para rotas 610, não forçar "Reentrega" aqui. A decisão fica com o fluxo GM (Código.js).
    return false;
  } else {
    let gmNoCliente = false;
    let gmCritico = false;
    gmRows.forEach((row) => {
      const arrival = row["stop.actualArrival"];
      const departure = row["stop.actualDeparture"];
      if (temHorario(arrival) && !temHorario(departure)) {
        const arrivalMs = parseHorarioMs(arrival);
        if (arrivalMs && arrivalMs + RUNNER_DURACAO_MINUTOS_NO_CLIENTE * 60000 < Date.now()) {
          gmCritico = true;
        } else {
          gmNoCliente = true;
        }
      }
    });

    if (gmCritico || temCritico) {
      statusEsperado = "Crítico";
    } else if (gmNoCliente || temNoCliente) {
      statusEsperado = "No Cliente";
    } else if (todosFinalizados) {
      statusEsperado = "Validar Finalização";
    } else {
      statusEsperado = "Em rota";
    }
  }

  if (!statusEsperado) return false;
  if (lowerAtual === statusEsperado.toLowerCase()) return false;

  console.log(`🔁 Reconciliando status da task ${taskId}: "${statusAtual}" -> "${statusEsperado}"`);
  return atualizarTaskClickUpRunner(taskId, { status: statusEsperado }, `status "${statusEsperado}"`);
}

function temHorario(valor) {
  if (valor === null || valor === undefined) return false;
  if (valor instanceof Date) return true;
  if (typeof valor === "number") return true;
  return String(valor).trim().length > 0;
}

function parseHorarioMs(valor) {
  if (!valor) return null;
  if (valor instanceof Date) return valor.getTime();
  if (typeof valor === "number") return valor;
  const date = new Date(valor);
  if (Number.isNaN(date.getTime())) return null;
  return date.getTime();
}

function aplicarReentregaSemGreenMile(task) {
  if (!task || !task.id) return;
  const taskId = task.id;
  const subtasks = Array.isArray(task.subtasks) ? task.subtasks : [];
  if (subtasks.length === 0) return;

  const subtasksDetalhes = buscarSubtasksDetalhes(subtasks.map(st => st.id));
  const nfesPorSubtask = [];
  const updatesStatus = [];

  subtasksDetalhes.forEach((subtask) => {
    if (!subtask || !subtask.id) return;

    // Se tem ID GM, normalmente seria uma subtask "normal" de entrega.
    // Mas se estamos em "aplicarReentregaSemGreenMile" (chamado quando rotaNaoGM == true),
    // então presumimos que é uma reentrega manual/pura ou cópia de task antiga.
    // Portanto, IGNORAMOS o fato de ter idGM e forçamos Reentrega.
    const idGM = extrairIdGM(subtask);
    if (idGM) {
      // Apenas loga para debug, mas NÃO retorna.
      // console.log(`[WF] Subtask ${subtask.id} tem ID GM (${idGM}) mas será tratada como Reentrega (Sem GM).`);
    }

    const nfChecklist = extrairNfesChecklist(subtask);
    const { fieldId, fieldValue } = extrairCampoNfe(subtask);
    const mergedNfes = mergeNfes(fieldValue, nfChecklist);

    if (mergedNfes) {
      nfesPorSubtask.push(mergedNfes);
      if (fieldId && precisaAtualizarCampo(fieldValue, mergedNfes)) {
        atualizarCampoPersonalizadoClickUpRunner(subtask.id, fieldId, mergedNfes);
      }
    }

    const currentStatus = subtask.status ? subtask.status.status : "";

    // START CHANGE: Check if already finalized
    if (isStatusFinal(currentStatus)) {
      // console.log(`[WF] Subtask ${subtask.id} already finalized ("${currentStatus}"), skipping force re-delivery status.`);
      return;
    }
    // END CHANGE

    updatesStatus.push({ subtaskId: subtask.id, novoStatus: RUNNER_REENTREGA_STATUS });
  });

  if (updatesStatus.length > 0) {
    atualizarStatusSubtasksBatch(updatesStatus);
  }

  const mergedTaskNfes = mergeNfes("", nfesPorSubtask.join(", "));
  if (mergedTaskNfes) {
    const { fieldId: fieldIdTask, fieldValue: fieldValueTask } = extrairCampoNfe(task);
    if (fieldIdTask && precisaAtualizarCampo(fieldValueTask, mergedTaskNfes)) {
      atualizarCampoPersonalizadoClickUpRunner(taskId, fieldIdTask, mergedTaskNfes);
    }
  }
}

function extrairIdGM(task) {
  if (task && Array.isArray(task.custom_fields)) {
    const campo = task.custom_fields.find(cf =>
      cf.name && cf.name.includes("ID GM Localização")
    );
    if (campo && campo.value) {
      return String(campo.value).trim();
    }
  }
  return null;
}

function buscarSubtasksDetalhes(subtaskIds) {
  if (!Array.isArray(subtaskIds) || subtaskIds.length === 0) return [];
  const requests = subtaskIds.map((id) => ({
    url: `https://api.clickup.com/api/v2/task/${id}`,
    method: "GET"
  }));
  const responses = cuFetchBatch_("cu-subtask", requests);
  const result = [];
  responses.forEach((response) => {
    if (response.getResponseCode() !== 200) return;
    try {
      result.push(JSON.parse(response.getContentText()));
    } catch (e) { }
  });
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
  task.checklists.forEach((checklist) => {
    if (checklist.items && Array.isArray(checklist.items)) {
      checklist.items.forEach((item) => {
        if (item.name) itens.push(item.name.trim());
      });
    }
  });
  return itens.join(", ");
}

function mergeNfes(valorAtual, valorChecklist) {
  const set = new Set();
  [valorAtual, valorChecklist].forEach((valor) => {
    if (!valor) return;
    String(valor).split(",").forEach((parte) => {
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

function atualizarCampoPersonalizadoClickUpRunner(taskId, fieldId, valor) {
  if (!taskId || !fieldId) return false;
  const url = `https://api.clickup.com/api/v2/task/${taskId}/field/${fieldId}`;
  const options = {
    method: "POST",
    payload: JSON.stringify({ value: valor })
  };
  try {
    const response = cuFetch_("cu-field", url, options);
    const code = response.getResponseCode();
    if (code === 200 || code === 204) {
      markClickUpPatched();
      console.log(`   ✅ Campo personalizado atualizado (task ${taskId})`);
      return true;
    }
    console.warn(`   ⚠️ Erro ao atualizar campo (task ${taskId}): ${code}`);
  } catch (e) {
    console.error(`   ❌ Erro ao atualizar campo (task ${taskId}): ${e.message}`);
  }
  return false;
}

function atualizarStatusSubtasksBatch(updates) {
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

function atualizarTaskClickUpRunner(taskId, payload, label) {
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

function logHttpResponse(where, url, response, counterKey) {
  const code = response.getResponseCode();
  const body = sanitizeBody(response.getContentText());
  console.log(`[${where}] ${url} -> ${code} | body=${body}`);
  if (counterKey && code >= 400) {
    incrementTelemetryCounter(counterKey);
  }
}

function logHttpError(where, url, error) {
  console.error(`[${where}] ${url} -> erro ${error.message}`);
}

function sanitizeBody(body) {
  if (typeof body !== "string") return "";
  if (body.length <= MAX_LOG_BODY_LENGTH) return body;
  return `${body.slice(0, MAX_LOG_BODY_LENGTH)}...`;
}

function handleRunnerHealth(status) {
  const streak = Number(PropertiesService.getScriptProperties().getProperty(PROPERTY_RUNNER_ERROR_STREAK) || "0");
  const nextStreak = status === "error" ? streak + 1 : 0;
  PropertiesService.getScriptProperties().setProperty(PROPERTY_RUNNER_ERROR_STREAK, String(nextStreak));
  if (nextStreak >= 3) {
    console.warn(`[WF] runner falhou ${nextStreak} execuções seguidas; revisar tokens/serviços.`);
  }
}

function extractRouteKey(task) {
  if (!task || !task.name) return null;
  let name = String(task.name || "").trim();
  if (name.includes("-")) {
    name = name.split("-")[0].trim();
  }
  if (!name.startsWith("610")) return null;
  return name;
}

function computeFingerprint(routeKey, flatItems) {
  const normalized = (flatItems || []).map((flatItem) => {
    const entry = {};
    Object.keys(flatItem).sort().forEach((key) => {
      if (key.startsWith("clickup.")) return;
      const value = flatItem[key];
      if (value instanceof Date) {
        entry[key] = value.toISOString();
      } else if (value && typeof value === "object") {
        entry[key] = JSON.stringify(value);
      } else {
        entry[key] = value !== undefined && value !== null ? String(value) : "";
      }
    });
    return entry;
  });
  normalized.sort((a, b) => {
    const stopKeyA = a["stop.location.key"] || "";
    const stopKeyB = b["stop.location.key"] || "";
    if (stopKeyA !== stopKeyB) return stopKeyA < stopKeyB ? -1 : 1;
    const orderA = Number(a["stop.plannedSequenceNum"]) || 0;
    const orderB = Number(b["stop.plannedSequenceNum"]) || 0;
    return orderA - orderB;
  });
  const payload = JSON.stringify({ route: routeKey, rows: normalized });
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, payload, Utilities.Charset.UTF_8);
  return digest.map((byte) => (`0${(byte & 0xFF).toString(16)}`).slice(-2)).join('');
}

function getRouteFingerprint(routeKey) {
  if (!routeKey) return null;
  return PropertiesService.getScriptProperties().getProperty(PROPERTY_FINGERPRINT_PREFIX + routeKey);
}

function setRouteFingerprint(routeKey, fingerprint) {
  if (!routeKey || !fingerprint) return;
  PropertiesService.getScriptProperties().setProperty(PROPERTY_FINGERPRINT_PREFIX + routeKey, fingerprint);
}

function runRunnerManual() {
  runnerMain();
}

function validarIntegracoesLeves() {
  const clickUpToken = getConfiguredValue("CLICKUP_TOKEN", DEFAULT_CLICKUP_TOKEN);
  const gmUser = getConfiguredValue("GM_USERNAME", DEFAULT_GM_USERNAME);
  const gmPassword = getConfiguredValue("GM_PASSWORD", DEFAULT_GM_PASSWORD);
  const sampleFlat = [
    {
      "route.key": "6100000",
      "stop.location.key": "LOC-1",
      "stop.plannedSequenceNum": 1,
      "stop.actualArrival": "2025-01-01T10:00:00.000Z",
      "stop.actualDeparture": "2025-01-01T10:30:00.000Z",
      "stop.deliveryStatus": "DELIVERED"
    }
  ];
  const fp1 = computeFingerprint("6100000", sampleFlat);
  const fp2 = computeFingerprint("6100000", sampleFlat.map(item => Object.assign({}, item)));
  const operationalWindowOK = isWithinOperationalWindow(new Date(Date.now() + RUNNER_INTERVAL_MS));
  return {
    clickUpTokenConfigured: !!clickUpToken,
    gmCredentialsConfigured: !!gmUser && !!gmPassword,
    fingerprintStable: fp1 === fp2,
    nextRunInWindow: operationalWindowOK
  };
}



// ============================================================================
// DASHBOARD V2 FUNCTIONS
// ============================================================================

/**
 * Get full task from prefetch cache (with custom_fields and subtasks)
 */
function getFullTaskFromPrefetch(taskId, scriptCache) {
  // Priority 1: In-memory prefetch cache
  if (typeof PREFETCH_TASKS_BY_ID !== "undefined" && PREFETCH_TASKS_BY_ID && PREFETCH_TASKS_BY_ID[taskId]) {
    return PREFETCH_TASKS_BY_ID[taskId];
  }
  // Priority 2: Script cache
  if (scriptCache) {
    try {
      const cached = scriptCache.get(`cu_task_${taskId}`);
      if (cached) {
        return JSON.parse(cached);
      }
    } catch (e) { }
  }
  return null;
}

/**
 * Extract custom field value using ID priority, then pattern matching
 */
function extractCF(task, fieldKey) {
  if (!task || !task.custom_fields || !Array.isArray(task.custom_fields)) return null;

  // Priority 1: Match by known field ID
  const knownId = CF_IDS[fieldKey];
  if (knownId) {
    const byId = task.custom_fields.find(f => f.id === knownId);
    if (byId) return extractCFValue(byId);
  }

  // Priority 2: Match by specific patterns (most specific first)
  const patterns = CF_PATTERNS[fieldKey];
  if (patterns) {
    for (const pattern of patterns) {
      const cf = task.custom_fields.find(f =>
        f.name && normalizeFieldName(f.name).includes(pattern.toLowerCase())
      );
      if (cf) return extractCFValue(cf);
    }
  }

  return null;
}

/**
 * Normalize field name for matching (remove emojis, lowercase)
 */
function normalizeFieldName(name) {
  if (!name) return "";
  return name.toLowerCase()
    .replace(/[\u{1F300}-\u{1F9FF}]/gu, "")
    .replace(/[🤖🚚🗺️📍📋📅🔴🚗🚛]/g, "")
    .trim();
}

/**
 * Extract value from custom field based on type
 */
function extractCFValue(cf) {
  if (!cf || cf.value === undefined || cf.value === null) return null;

  if (cf.type === "drop_down" && cf.type_config && cf.type_config.options) {
    let opt = null;
    if (typeof cf.value === "string") {
      opt = cf.type_config.options.find(o => o.id === cf.value);
    }
    if (!opt && typeof cf.value === "number") {
      opt = cf.type_config.options.find(o => o.orderindex === cf.value);
    }
    if (!opt && typeof cf.value === "number" && cf.type_config.options[cf.value]) {
      opt = cf.type_config.options[cf.value];
    }
    return opt ? opt.name : String(cf.value);
  }

  if (cf.type === "automatic_progress") {
    return cf.value ? { percent_complete: cf.value.percent_complete || 0 } : { percent_complete: 0 };
  }

  if (cf.type === "date" && cf.value) {
    return parseDateSafe(cf.value);
  }

  if (cf.type === "checkbox") {
    return cf.value === true || cf.value === "true";
  }

  if (cf.type === "labels" && Array.isArray(cf.value)) {
    return cf.value.map(v => v.label || v).join(", ");
  }

  return cf.value;
}

/**
 * Parse date defensively (handles epoch ms, epoch s, ISO strings, Date objects)
 */
function parseDateSafe(value) {
  if (!value) return null;

  if (typeof value === "number") {
    if (value > 1577836800000 && value < 4102444800000) {
      return value;
    }
    if (value > 1577836800 && value < 4102444800) {
      return value * 1000;
    }
    return null;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d+$/.test(trimmed)) {
      const num = parseInt(trimmed, 10);
      return parseDateSafe(num);
    }
    try {
      const date = new Date(trimmed);
      if (!isNaN(date.getTime())) {
        return date.getTime();
      }
    } catch (e) { }
  }

  if (value instanceof Date) {
    return value.getTime();
  }

  return null;
}

/**
 * Check if value is truthy (for checkbox fields)
 */
function isTruthyValue(val) {
  if (val === true) return true;
  if (typeof val === "string") {
    const lower = val.toLowerCase().trim();
    return lower === "true" || lower === "yes" || lower === "sim" || lower === "1";
  }
  return false;
}

/**
 * Build complete dashboard model from tasks and snapshots
 */
function buildDashModel(tasks, routeMap, snapshots, scriptProps, telemetry) {
  const now = Date.now();
  const dataVer = getCurrentDataVersion(scriptProps);
  const scriptCache = CacheService.getScriptCache();

  const planos = [];
  const clientesSummary = {};
  let totalEntregasPlanejadas = 0;
  let totalEntregasFeitas = 0;
  let ocorrenciasCount = 0;
  let sinistroCount = 0;
  let pernoiteCount = 0;
  const planosPorRegiao = {};
  const statusPlanosDistribuicao = {};
  const statusClientesDistribuicao = {};
  const temposAtivos = [];
  const temposFinalizados = [];

  tasks.forEach(task => {
    if (!task || !task.id) return;

    const fullTask = getFullTaskFromPrefetch(task.id, scriptCache) || task;

    const routeKey = extractRouteKey(fullTask);
    const routeInfo = routeKey ? routeMap.get(routeKey) : null;
    const snapshot = routeKey ? snapshots[routeKey] : null;

    const plano = extractCF(fullTask, "PLANO") || routeKey || fullTask.name;
    const motorista = extractCF(fullTask, "MOTORISTA");
    const placa = extractCF(fullTask, "PLACA");
    const unidade = extractCF(fullTask, "UNIDADE");
    const veiculo = extractCF(fullTask, "VEICULO");
    const modalidade = extractCF(fullTask, "MODALIDADE");
    const regiao = extractCF(fullTask, "REGIAO");
    const saida = extractCF(fullTask, "DATA_SAIDA");
    const qtdEntregas = extractCF(fullTask, "QTD_ENTREGAS");
    const ultimaLocalizacao = extractCF(fullTask, "ULTIMA_LOCALIZACAO");
    const progressoRaw = extractCF(fullTask, "PROGRESSO");
    const ocorrencias = extractCF(fullTask, "OCORRENCIAS");
    const sinistro = extractCF(fullTask, "SINISTRO");
    const pernoite = extractCF(fullTask, "PERNOITE");

    let entregasFeitas = 0, entregasPlanejadas = 0;
    if (qtdEntregas) {
      const match = String(qtdEntregas).match(/^(\d+)\/(\d+)/);
      if (match) {
        entregasFeitas = parseInt(match[1], 10);
        entregasPlanejadas = parseInt(match[2], 10);
      }
    }

    if (entregasPlanejadas === 0 && snapshot && snapshot.items) {
      entregasPlanejadas = snapshot.items.length;
      const delivered = snapshot.items.filter(s => {
        const st = String(s["stop.deliveryStatus"] || "").toUpperCase();
        return st === "DELIVERED" || st === "COMPLETE";
      }).length;
      entregasFeitas = delivered;
    }

    totalEntregasPlanejadas += entregasPlanejadas;
    totalEntregasFeitas += entregasFeitas;

    let progressoPercent = 0;
    if (progressoRaw && typeof progressoRaw === "object" && progressoRaw.percent_complete !== undefined) {
      progressoPercent = progressoRaw.percent_complete;
    } else if (entregasPlanejadas > 0) {
      progressoPercent = Math.round((entregasFeitas / entregasPlanejadas) * 100);
    }

    const ocorrenciasFlag = isTruthyValue(ocorrencias);
    const sinistroFlag = isTruthyValue(sinistro);
    const pernoiteFlag = isTruthyValue(pernoite);

    if (ocorrenciasFlag) ocorrenciasCount++;
    if (sinistroFlag) sinistroCount++;
    if (pernoiteFlag) pernoiteCount++;

    const statusTask = fullTask.status ? fullTask.status.status : "Em rota";
    const statusNormalized = String(statusTask).toLowerCase();
    const isClosed = statusNormalized === "closed" || statusNormalized === "complete" || statusNormalized === "finalizado" || statusNormalized === "concluído" || statusNormalized === "entregue";

    const regiaoKey = regiao || "Sem Região";
    // Count stats? If closed, maybe separate stats? 
    // For now, keep them in distributions.

    // ... (keep distributions logic) ...
    planosPorRegiao[regiaoKey] = (planosPorRegiao[regiaoKey] || 0) + 1;
    statusPlanosDistribuicao[statusTask] = (statusPlanosDistribuicao[statusTask] || 0) + 1;

    // ... (rest of loop) ...

    planos.push({
      taskId: fullTask.id,
      routeKey: routeKey || plano,
      taskUrl: routeInfo ? routeInfo.taskUrl : `https://app.clickup.com/t/${fullTask.id}`,
      motorista: String(motorista || "").substring(0, 50),
      placa: placa || "",
      unidade: unidade || "",
      veiculo: veiculo || "",
      modalidade: modalidade || "",
      regiao: regiaoKey,
      saida: saida || null,
      entregasPlanejadas: entregasPlanejadas,
      entregasFeitas: entregasFeitas,
      progressoPercent: progressoPercent,
      ultimaLocalizacao: String(ultimaLocalizacao || "").substring(0, 100),
      statusTask: statusTask,
      flags: (ocorrenciasFlag ? "O" : "") + (sinistroFlag ? "S" : "") + (pernoiteFlag ? "P" : ""),
      clientesCount: clientesMini.length,
      isClosed: isClosed,
      dateUpdated: Number(fullTask.date_updated) || 0 // For filtering
    });
  });

  // ... (bucket logic from previous step) ...

  const bucketsAtraso = {
    "0-30": 0,
    "31-60": 0,
    "61-120": 0,
    "121+": 0
  };

  temposAtivos.forEach(t => {
    if (t.tempoMin <= 30) bucketsAtraso["0-30"]++;
    else if (t.tempoMin <= 60) bucketsAtraso["31-60"]++;
    else if (t.tempoMin <= 120) bucketsAtraso["61-120"]++;
    else bucketsAtraso["121+"]++;
  });

  return {
    generatedAt: now,
    dataVer: String(dataVer),
    execId: telemetry.execId,
    planos: planos,
    clientes: clientesSummary,
    metricas: {
      totalPlanosAtivos: planos.filter(p => !p.isClosed).length,
      totalPlanosHistory: planos.length,
      totalClientes: Object.values(clientesSummary).reduce((sum, arr) => sum + arr.length, 0),
      // ... (rest) ...
      totalEntregasPlanejadas: totalEntregasPlanejadas,
      totalEntregasFeitas: totalEntregasFeitas,
      progressoMedioPercent: progressoMedioPercent,
      planosPorRegiao: planosPorRegiao,
      statusPlanosDistribuicao: statusPlanosDistribuicao,
      statusClientesDistribuicao: statusClientesDistribuicao,
      ocorrenciasCount: ocorrenciasCount,
      sinistroCount: sinistroCount,
      pernoiteCount: pernoiteCount,
      tempoMedioNoClienteMin: tempoMedioNoClienteMin,
      p95NoClienteMin: p95NoClienteMin,
      totalTemposAtivos: temposAtivos.length,
      totalTemposFinalizados: temposFinalizados.length,
      rankingTopAtrasosAtivos: rankingTopAtrasosAtivos,
      rankingTopTemposFinalizados: rankingTopTemposFinalizados,
      bucketsAtraso: bucketsAtraso
    }
  };
}

/**
 * Smart cleanup of previous cache chunks (uses index first, then fallback scan)
 */
function clearDashV2CacheChunksSmart(scriptProps) {
  const prevIndexRaw = scriptProps.getProperty(PROPERTY_DASH_V2_CACHE_INDEX);

  if (prevIndexRaw) {
    try {
      const prevIndex = JSON.parse(prevIndexRaw);
      const prevN = prevIndex.n || 0;

      for (let i = 0; i < prevN; i++) {
        scriptProps.deleteProperty(PROPERTY_DASH_V2_CACHE_PREFIX + i);
      }
      console.log(`[WF] Cleared ${prevN} previous DashV2 cache chunks`);
      return;
    } catch (e) {
      console.warn(`[WF] Could not parse previous index, falling back to scan`);
    }
  }

  const allProps = scriptProps.getProperties();
  let cleared = 0;
  Object.keys(allProps).forEach(key => {
    if (key.startsWith(PROPERTY_DASH_V2_CACHE_PREFIX)) {
      scriptProps.deleteProperty(key);
      cleared++;
    }
  });
  if (cleared > 0) {
    console.log(`[WF] Cleared ${cleared} DashV2 cache chunks via scan fallback`);
  }
}

/**
 * Persist dashboard model to chunked cache storage
 */
function persistDashV2Cache(dashModel, scriptProps) {
  if (!dashModel) return;

  try {
    const json = JSON.stringify(dashModel);
    const chunks = [];

    for (let i = 0; i < json.length; i += DASH_V2_CHUNK_SIZE) {
      chunks.push(json.substring(i, i + DASH_V2_CHUNK_SIZE));
    }

    clearDashV2CacheChunksSmart(scriptProps);

    chunks.forEach((chunk, idx) => {
      scriptProps.setProperty(PROPERTY_DASH_V2_CACHE_PREFIX + idx, chunk);
    });

    const index = {
      n: chunks.length,
      ts: Date.now(),
      dataVer: dashModel.dataVer,
      execId: dashModel.execId,
      bytes: json.length
    };
    scriptProps.setProperty(PROPERTY_DASH_V2_CACHE_INDEX, JSON.stringify(index));

    console.log(`[WF] DashV2 cache persisted: ${chunks.length} chunk(s), ${json.length} bytes`);
  } catch (e) {
    console.error(`[WF] Failed to persist DashV2 cache: ${e.message}`);
  }
}

/**
 * Read dashboard model from chunked cache storage
 */
function readDashV2Cache(scriptProps) {
  const indexRaw = scriptProps.getProperty(PROPERTY_DASH_V2_CACHE_INDEX);
  if (!indexRaw) return null;

  try {
    const index = JSON.parse(indexRaw);
    const chunks = [];

    for (let i = 0; i < index.n; i++) {
      const chunk = scriptProps.getProperty(PROPERTY_DASH_V2_CACHE_PREFIX + i);
      if (!chunk) {
        console.warn(`[WF] DashV2 cache chunk ${i} missing`);
        return null;
      }
      chunks.push(chunk);
    }

    const json = chunks.join("");
    return {
      data: JSON.parse(json),
      index: index
    };
  } catch (e) {
    console.error(`[WF] Failed to read DashV2 cache: ${e.message}`);
    return null;
  }
}
