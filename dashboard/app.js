/**
 * ============================================
 * Dashboard de Rotas - JavaScript Application
 * ============================================
 */

// ===== Configuration =====
const CONFIG = {
    // Apps Script Web App URL - UPDATE THIS with your deployed URL
    API_URL: '',

    // Refresh interval in milliseconds (30 seconds)
    REFRESH_INTERVAL: 30000,

    // Status mapping
    STATUS_MAP: {
        'finalizada': 'finalizado',
        'finalizado': 'finalizado',
        'done': 'finalizado',
        'complete': 'finalizado',
        'entregue': 'finalizado',
        'em rota': 'emrota',
        'em andamento': 'emrota',
        'in progress': 'emrota',
        'no cliente': 'nocliente',
        'critical': 'critico',
        'crítico': 'critico',
        'critico': 'critico',
        'retorno': 'retorno',
        'returned': 'retorno',
        'reentrega': 'reentrega',
        'redelivery': 'reentrega'
    }
};

// ===== State =====
let state = {
    routes: [],
    filteredRoutes: [],
    currentFilter: 'all',
    searchQuery: '',
    isLoading: true,
    isConnected: false,
    lastUpdate: null
};

// ===== DOM Elements =====
const DOM = {
    // KPIs
    kpiRotasAtivas: document.getElementById('kpiRotasAtivas'),
    kpiFinalizadas: document.getElementById('kpiFinalizadas'),
    kpiEmRota: document.getElementById('kpiEmRota'),
    kpiNoCliente: document.getElementById('kpiNoCliente'),
    kpiCritico: document.getElementById('kpiCritico'),
    kpiRetorno: document.getElementById('kpiRetorno'),
    kpiReentrega: document.getElementById('kpiReentrega'),
    progressFinalizadas: document.getElementById('progressFinalizadas'),

    // Table
    routesTableBody: document.getElementById('routesTableBody'),
    tableEmpty: document.getElementById('tableEmpty'),

    // Controls
    searchInput: document.getElementById('searchInput'),
    filterChips: document.getElementById('filterChips'),
    btnRefresh: document.getElementById('btnRefresh'),

    // Status
    lastUpdate: document.getElementById('lastUpdate'),
    connectionStatus: document.getElementById('connectionStatus'),
    loadingOverlay: document.getElementById('loadingOverlay'),
    toastContainer: document.getElementById('toastContainer')
};

// ===== Demo Data (used when no API configured) =====
const DEMO_DATA = {
    dataVer: "1",
    snapshotTs: Date.now(),
    items: [
        {
            routeKey: "610001234",
            fingerprint: "abc123",
            rows: 8,
            items: [
                { "route.key": "610001234", "stop.location.description": "Mercado Central", "stop.location.addressLine1": "Rua das Flores, 123", "stop.actualArrival": new Date().toISOString(), "stop.actualDeparture": null, "stop.deliveryStatus": "IN_PROGRESS", "clickup.subtaskStatus": "No Cliente", "stop.plannedSequenceNum": 1 },
                { "route.key": "610001234", "stop.location.description": "Supermercado ABC", "stop.location.addressLine1": "Av. Brasil, 456", "stop.actualArrival": new Date(Date.now() - 3600000).toISOString(), "stop.actualDeparture": new Date(Date.now() - 3300000).toISOString(), "stop.deliveryStatus": "DELIVERED", "clickup.subtaskStatus": "Finalizada", "stop.plannedSequenceNum": 2 },
                { "route.key": "610001234", "stop.location.description": "Padaria São João", "stop.location.addressLine1": "Rua XV, 789", "stop.actualArrival": null, "stop.actualDeparture": null, "stop.deliveryStatus": "PENDING", "clickup.subtaskStatus": "Em Rota", "stop.plannedSequenceNum": 3 }
            ]
        },
        {
            routeKey: "610001235",
            fingerprint: "def456",
            rows: 5,
            items: [
                { "route.key": "610001235", "stop.location.description": "Loja do Centro", "stop.location.addressLine1": "Praça Central, 10", "stop.actualArrival": new Date(Date.now() - 7200000).toISOString(), "stop.actualDeparture": new Date(Date.now() - 7000000).toISOString(), "stop.deliveryStatus": "DELIVERED", "clickup.subtaskStatus": "Finalizada", "stop.plannedSequenceNum": 1 },
                { "route.key": "610001235", "stop.location.description": "Farmácia Popular", "stop.location.addressLine1": "Rua A, 50", "stop.actualArrival": new Date(Date.now() - 6000000).toISOString(), "stop.actualDeparture": new Date(Date.now() - 5800000).toISOString(), "stop.deliveryStatus": "UNDELIVERED", "clickup.subtaskStatus": "Retorno", "stop.undeliverableCode.description": "Cliente ausente", "stop.plannedSequenceNum": 2 }
            ]
        },
        {
            routeKey: "610001236",
            fingerprint: "ghi789",
            rows: 10,
            items: [
                { "route.key": "610001236", "stop.location.description": "Atacadão Norte", "stop.location.addressLine1": "Rod. BR-101, km 50", "stop.actualArrival": new Date(Date.now() - 2700000).toISOString(), "stop.actualDeparture": null, "stop.deliveryStatus": "IN_PROGRESS", "clickup.subtaskStatus": "Crítico", "stop.plannedSequenceNum": 1 }
            ]
        },
        {
            routeKey: "REENTREGA-001",
            fingerprint: "jkl012",
            rows: 3,
            items: [
                { "route.key": "REENTREGA-001", "stop.location.description": "Cliente Especial", "stop.location.addressLine1": "Av. Industrial, 1000", "stop.actualArrival": null, "stop.actualDeparture": null, "stop.deliveryStatus": "PENDING", "clickup.subtaskStatus": "Reentrega", "stop.plannedSequenceNum": 1 }
            ]
        }
    ]
};

// ===== Utility Functions =====
function normalizeStatus(status) {
    if (!status) return 'emrota';
    const lower = status.toLowerCase().trim();
    return CONFIG.STATUS_MAP[lower] || 'emrota';
}

function formatTime(date) {
    if (!date) return '--:--';
    const d = new Date(date);
    return d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
}

function formatTimeAgo(date) {
    if (!date) return 'Nunca';
    const now = new Date();
    const d = new Date(date);
    const diff = Math.floor((now - d) / 1000);

    if (diff < 60) return 'Agora';
    if (diff < 3600) return `${Math.floor(diff / 60)}min atrás`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h atrás`;
    return d.toLocaleDateString('pt-BR');
}

function animateValue(element, target, duration = 500) {
    const start = parseInt(element.textContent) || 0;
    const increment = (target - start) / (duration / 16);
    let current = start;

    const timer = setInterval(() => {
        current += increment;
        if ((increment > 0 && current >= target) || (increment < 0 && current <= target)) {
            element.textContent = target;
            clearInterval(timer);
        } else {
            element.textContent = Math.round(current);
        }
    }, 16);
}

// ===== Data Processing =====
function processRoutes(data) {
    if (!data || !data.items) return [];

    return data.items.map(route => {
        const stops = route.items || [];
        const total = stops.length;

        // Count by status
        let finalizadas = 0;
        let retornos = 0;
        let noCliente = 0;
        let critico = 0;
        let reentrega = 0;
        let currentClient = null;
        let lastUpdate = null;

        stops.forEach(stop => {
            const status = normalizeStatus(stop['clickup.subtaskStatus']);
            const arrival = stop['stop.actualArrival'];
            const departure = stop['stop.actualDeparture'];

            if (status === 'finalizado') finalizadas++;
            else if (status === 'retorno') retornos++;
            else if (status === 'nocliente') noCliente++;
            else if (status === 'critico') critico++;
            else if (status === 'reentrega') reentrega++;

            // Find current client (arrived but not departed)
            if (arrival && !departure) {
                currentClient = {
                    name: stop['stop.location.description'] || 'Cliente',
                    address: stop['stop.location.addressLine1'] || ''
                };
            }

            // Track last update
            const times = [arrival, departure].filter(Boolean).map(t => new Date(t).getTime());
            const maxTime = Math.max(...times, 0);
            if (maxTime > (lastUpdate || 0)) lastUpdate = maxTime;
        });

        // Determine overall route status
        let routeStatus = 'emrota';
        if (critico > 0) routeStatus = 'critico';
        else if (noCliente > 0) routeStatus = 'nocliente';
        else if (reentrega > 0 && finalizadas === 0) routeStatus = 'reentrega';
        else if (finalizadas === total && total > 0) routeStatus = 'finalizado';
        else if (retornos > 0 && finalizadas + retornos === total) routeStatus = 'retorno';

        return {
            key: route.routeKey,
            status: routeStatus,
            total,
            finalizadas,
            retornos,
            noCliente,
            critico,
            reentrega,
            progress: total > 0 ? Math.round((finalizadas / total) * 100) : 0,
            currentClient,
            lastUpdate: lastUpdate ? new Date(lastUpdate) : null
        };
    });
}

function calculateKPIs(routes) {
    return {
        total: routes.length,
        finalizadas: routes.reduce((sum, r) => sum + r.finalizadas, 0),
        emRota: routes.filter(r => r.status === 'emrota').length,
        noCliente: routes.filter(r => r.status === 'nocliente').length,
        critico: routes.filter(r => r.status === 'critico').length,
        retorno: routes.reduce((sum, r) => sum + r.retornos, 0),
        reentrega: routes.filter(r => r.status === 'reentrega').length,
        totalStops: routes.reduce((sum, r) => sum + r.total, 0)
    };
}

// ===== Rendering =====
function renderKPIs(kpis) {
    animateValue(DOM.kpiRotasAtivas, kpis.total);
    animateValue(DOM.kpiFinalizadas, kpis.finalizadas);
    animateValue(DOM.kpiEmRota, kpis.emRota);
    animateValue(DOM.kpiNoCliente, kpis.noCliente);
    animateValue(DOM.kpiCritico, kpis.critico);
    animateValue(DOM.kpiRetorno, kpis.retorno);
    animateValue(DOM.kpiReentrega, kpis.reentrega);

    // Progress bar
    const progressPercent = kpis.totalStops > 0
        ? Math.round((kpis.finalizadas / kpis.totalStops) * 100)
        : 0;
    DOM.progressFinalizadas.style.width = `${progressPercent}%`;
}

function renderTable(routes) {
    if (routes.length === 0) {
        DOM.routesTableBody.innerHTML = '';
        DOM.tableEmpty.classList.add('visible');
        return;
    }

    DOM.tableEmpty.classList.remove('visible');

    DOM.routesTableBody.innerHTML = routes.map(route => `
        <tr data-route="${route.key}">
            <td>
                <div class="route-name">
                    <span class="route-key">${route.key}</span>
                </div>
            </td>
            <td>
                <span class="status-badge ${route.status}">
                    ${getStatusLabel(route.status)}
                </span>
            </td>
            <td>
                <div class="cell-progress">
                    <div class="progress-mini">
                        <div class="progress-mini-bar" style="width: ${route.progress}%"></div>
                    </div>
                    <span class="progress-text">${route.finalizadas}/${route.total}</span>
                </div>
            </td>
            <td>
                ${route.currentClient ? `
                    <div class="client-info">
                        <span class="client-name">${route.currentClient.name}</span>
                        <span class="client-address">${route.currentClient.address}</span>
                    </div>
                ` : '<span class="time-ago">-</span>'}
            </td>
            <td>
                <span class="time-ago">${formatTimeAgo(route.lastUpdate)}</span>
            </td>
            <td>
                <a href="https://app.clickup.com" target="_blank" class="action-btn" title="Abrir no ClickUp">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
                        <polyline points="15 3 21 3 21 9"/>
                        <line x1="10" y1="14" x2="21" y2="3"/>
                    </svg>
                </a>
            </td>
        </tr>
    `).join('');
}

function getStatusLabel(status) {
    const labels = {
        'finalizado': '✓ Finalizado',
        'emrota': '🚚 Em Rota',
        'nocliente': '⏰ No Cliente',
        'critico': '🔴 Crítico',
        'retorno': '↩️ Retorno',
        'reentrega': '♻️ Reentrega'
    };
    return labels[status] || status;
}

function updateLastUpdateTime() {
    state.lastUpdate = new Date();
    DOM.lastUpdate.innerHTML = `
        <span class="pulse-dot"></span>
        <span>Atualizado: ${formatTime(state.lastUpdate)}</span>
    `;
}

function updateConnectionStatus(isConnected) {
    state.isConnected = isConnected;
    DOM.connectionStatus.innerHTML = isConnected
        ? '<span class="status-dot online"></span> Conectado'
        : '<span class="status-dot offline"></span> Desconectado';
}

// ===== Filtering =====
function applyFilters() {
    let filtered = [...state.routes];

    // Apply status filter
    if (state.currentFilter !== 'all') {
        filtered = filtered.filter(r => r.status === state.currentFilter);
    }

    // Apply search
    if (state.searchQuery) {
        const query = state.searchQuery.toLowerCase();
        filtered = filtered.filter(r =>
            r.key.toLowerCase().includes(query) ||
            (r.currentClient && r.currentClient.name.toLowerCase().includes(query))
        );
    }

    state.filteredRoutes = filtered;
    renderTable(filtered);
}

// ===== API =====
async function fetchData() {
    try {
        DOM.btnRefresh.classList.add('loading');

        let data;

        if (CONFIG.API_URL) {
            const response = await fetch(CONFIG.API_URL);
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const json = await response.json();
            data = json.data || json;
        } else {
            // Use demo data
            await new Promise(resolve => setTimeout(resolve, 800)); // Simulate loading
            data = DEMO_DATA;
        }

        state.routes = processRoutes(data);
        const kpis = calculateKPIs(state.routes);

        renderKPIs(kpis);
        applyFilters();
        updateLastUpdateTime();
        updateConnectionStatus(true);

        if (!CONFIG.API_URL) {
            showToast('Modo demo ativo. Configure API_URL para dados reais.', 'info');
        }

    } catch (error) {
        console.error('Fetch error:', error);
        updateConnectionStatus(false);
        showToast(`Erro ao carregar dados: ${error.message}`, 'error');
    } finally {
        DOM.btnRefresh.classList.remove('loading');
        hideLoading();
    }
}

// ===== UI Helpers =====
function hideLoading() {
    DOM.loadingOverlay.classList.add('hidden');
}

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    DOM.toastContainer.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 4000);
}

// ===== Event Listeners =====
function setupEventListeners() {
    // Search
    DOM.searchInput.addEventListener('input', (e) => {
        state.searchQuery = e.target.value;
        applyFilters();
    });

    // Filter chips
    DOM.filterChips.addEventListener('click', (e) => {
        if (e.target.classList.contains('chip')) {
            document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
            e.target.classList.add('active');
            state.currentFilter = e.target.dataset.filter;
            applyFilters();
        }
    });

    // Refresh button
    DOM.btnRefresh.addEventListener('click', () => {
        fetchData();
    });

    // KPI card clicks
    document.querySelectorAll('.kpi-card').forEach(card => {
        card.addEventListener('click', () => {
            const status = card.dataset.status;
            if (status && status !== 'total') {
                document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
                const matchingChip = document.querySelector(`.chip[data-filter="${status}"]`);
                if (matchingChip) {
                    matchingChip.classList.add('active');
                    state.currentFilter = status;
                } else {
                    document.querySelector('.chip[data-filter="all"]').classList.add('active');
                    state.currentFilter = 'all';
                }
                applyFilters();
            }
        });
    });
}

// ===== Initialization =====
function init() {
    setupEventListeners();
    fetchData();

    // Auto refresh
    setInterval(fetchData, CONFIG.REFRESH_INTERVAL);

    console.log('🚚 Dashboard initialized');
    console.log('📡 API URL:', CONFIG.API_URL || 'Not configured (using demo data)');
}

// Start when DOM is ready
document.addEventListener('DOMContentLoaded', init);
