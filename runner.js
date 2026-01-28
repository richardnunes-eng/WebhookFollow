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
const MAX_LOG_BODY_LENGTH = 2000;
const GM_LOGIN_URL = "https://3coracoes.greenmile.com/login";

const RUNNER_OPERATIONAL_WINDOW = { startHour: 6, endHour: 19 };

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
    processRunnerTasks(PropertiesService.getScriptProperties(), telemetry);
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
      cuErrors: 0
    }
  };
}

function persistTelemetrySummary(telemetry) {
  const summary = {
    execId: telemetry.execId,
    status: telemetry.status,
    startTs: telemetry.startTs,
    endTs: telemetry.endTs,
    durationMs: telemetry.durationMs,
    counters: telemetry.counters
  };
  PropertiesService.getScriptProperties().setProperty(PROPERTY_LAST_SUMMARY, JSON.stringify(summary));
  console.log(`[WF][${telemetry.execId}] summary: ${JSON.stringify(summary)}`);
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
  const triggers = ScriptApp.getProjectTriggers().filter((trigger) => trigger.getHandlerFunction() === handler);
  triggers.forEach((trigger) => ScriptApp.deleteTrigger(trigger));
  const desiredDelay = delayMs || RUNNER_INTERVAL_MS;
  const actualDelay = resolveOperationalDelay(desiredDelay);
  ScriptApp.newTrigger(handler).timeBased().after(actualDelay).create();
  console.log(`[WF] Runner rescheduled for ${actualDelay} ms (requested ${desiredDelay} ms, removed ${triggers.length} old trigger(s)).`);
}

function resolveOperationalDelay(desiredDelay) {
  const targetTime = new Date(Date.now() + desiredDelay);
  if (isWithinOperationalWindow(targetTime)) {
    return desiredDelay;
  }
  const nextRun = new Date();
  nextRun.setHours(RUNNER_OPERATIONAL_WINDOW.startHour, 0, 0, 0);
  if (nextRun.getTime() <= Date.now()) {
    nextRun.setDate(nextRun.getDate() + 1);
  }
  return nextRun.getTime() - Date.now();
}

function isWithinOperationalWindow(timestamp) {
  const hour = timestamp.getHours();
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
  const responses = UrlFetchApp.fetchAll(sanitized);
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
