// Sanitização central — escapa todos os caracteres perigosos para HTML e atributos
function escHtml(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const serviceAliases = {
  "LAUDO DE VERIFICACA": "LAUDO DE VERIFICACAO",
  "LAUDO DE VERIFICA": "LAUDO DE VERIFICACAO",
  "LAUDO CAUTELAR VERI": "LAUDO CAUTELAR VERIFICACAO",
  "LAUDO CAUTELAR VE": "LAUDO CAUTELAR VERIFICACAO",
  "CAUTELAR COM ANALIS": "CAUTELAR COM ANALISE",
  "CAUTELAR COM ANAL": "CAUTELAR COM ANALISE",
  "LAUDO DE TRANSFEREN": "LAUDO DE TRANSFERENCIA",
  "LAUDO DE TRANSFERENC": "LAUDO DE TRANSFERENCIA"
};

const sampleRows = [
  ["13/04/2026", "I/PORSCHE 911 CARRERA", "FBA1I22", "MARIN IMPORT", "LAUDO DE VERIFICACA", "FA", "R$ 200,00"],
  ["13/04/2026", "VW NIVUS HL TSI", "CVI7H06", "EUROPAMOTORS", "LAUDO CAUTELAR", "AV", "R$ 350,00"],
  ["13/04/2026", "I/PEUGEOT 208 STYLE MT", "ROU0A07", "EUROPAMOTORS", "LAUDO CAUTELAR", "AV", "R$ 350,00"],
  ["13/04/2026", "M.BENZ C180FF", "FWX6J43", "EUROPAMOTORS", "LAUDO CAUTELAR", "AV", "R$ 350,00"],
  ["13/04/2026", "I/M.BENZ AMG CLA35 4M", "TIX2A50", "EUROPAMOTORS", "LAUDO CAUTELAR", "FA", "R$ 120,00"],
  ["13/04/2026", "I/AUDI RS6 AV 4.0TFSI P", "FMI7F06", "2B MOTORS", "LAUDO CAUTELAR VERI", "FA", "R$ 150,00"],
  ["13/04/2026", "JEEP COMPASS LIMITED S", "FDS7J83", "PARK MOTORS", "LAUDO CAUTELAR", "FA", "R$ 100,00"],
  ["13/04/2026", "MMC/PAJERO TR4 FLEX HP", "EMT8I08", "PARTICULAR MOEMA", "LAUDO CAUTELAR VERI", "AV", "R$ 400,00"],
  ["13/04/2026", "NISSAN KICKS SL CVT", "BEY3434", "PARTICULAR MOEMA", "LAUDO CAUTELAR", "AV", "R$ 300,00"],
  ["13/04/2026", "I/VW JETTA VARIANT", "FEH1717", "PARTICULAR MOEMA", "LAUDO CAUTELAR VERI", "AV", "R$ 400,00"],
  ["13/04/2026", "I/AUDI A4 2.0TFSI", "GHN1F73", "CAR CHASE", "CAUTELAR COM ANALIS", "FA", "R$ 150,00"],
  ["13/04/2026", "BMW/320I M SPORT FLEX", "BWB9C63", "CAR CHASE", "CAUTELAR COM ANALIS", "FA", "R$ 150,00"],
  ["13/04/2026", "I/M.BENZ AMGGTS", "FRW8G87", "CAR CHASE", "CAUTELAR COM ANALIS", "FA", "R$ 150,00"],
  ["13/04/2026", "I/VW FUSCA AA", "FOY0240", "ZUFFEN MOTORS", "LAUDO DE VERIFICACA", "AV", "R$ 200,00"],
  ["13/04/2026", "I/FORD MUSTANG GT", "PKW727O", "CMR VEICULOS LTDA", "CAUTELAR COM ANALIS", "FA", "R$ 170,00"],
  ["13/04/2026", "I/MINI COOPER S", "FEJ3I50", "CMR VEICULOS LTDA", "LAUDO CAUTELAR", "FA", "R$ 120,00"],
  ["13/04/2026", "FORD/MUSTANG GT", "PKW7270", "CMR VEICULOS LTDA", "CAUTELAR COM ANALIS", "FA", "R$ 170,00"]
];

// Detecta prefixo /u/<unidade> quando rodando no servidor Railway
const _pathMatch = window.location.pathname.match(/^(\/u\/[^/]+)/);
const apiBase = _pathMatch ? _pathMatch[1] : "";

const state = {
  records: [],
  sourceFiles: [],
  filter: "todos",
  conferencia: {},     // { recordId: { status, pdv_valor, pdv_fp, pdv_hora } }
  conferido: new Set(), // IDs confirmados manualmente apesar de divergencia/sem_pdv
  pdvBase: null,       // { dinheiro, debito, credito, pix } snapshot do PDV (base para Entradas)
};

// ── Snapshot/autosave: persistencia contra perda de dados ────────────────────
function _buildSnapshotPayload(origem) {
  const firstDate = state.records[0]?.data || "";
  return {
    data:        firstDate || "",
    arquivos:    [...state.sourceFiles],
    records:     state.records,
    conferencia: state.conferencia || {},
    conferido:   [...(state.conferido || [])],
    pdv_base:    state.pdvBase || null,
    origem:      origem || "autosave",
  };
}

async function saveSnapshotAsync(origem) {
  if (!state.records.length) return;
  try {
    const payload = _buildSnapshotPayload(origem);
    await apiFetch(`${apiBase}/api/snapshot`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.warn("[snapshot] falha ao salvar:", e.message);
  }
}

const LS_DRAFT_KEY = `astro.fechamento.draft.${apiBase || "default"}`;

let _lsDebounce = null;
function autosaveLocal() {
  if (_lsDebounce) clearTimeout(_lsDebounce);
  _lsDebounce = setTimeout(() => {
    try {
      if (!state.records.length) {
        localStorage.removeItem(LS_DRAFT_KEY);
        return;
      }
      const payload = _buildSnapshotPayload("autosave");
      payload.saved_at = new Date().toISOString();
      localStorage.setItem(LS_DRAFT_KEY, JSON.stringify(payload));
    } catch (e) { /* quota ou modo privado */ }
  }, 400);
}

function loadLocalDraft() {
  try {
    const raw = localStorage.getItem(LS_DRAFT_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function clearLocalDraft() {
  try { localStorage.removeItem(LS_DRAFT_KEY); } catch {}
}

function hydrateFromPayload(payload) {
  if (!payload) return;
  state.records     = Array.isArray(payload.records) ? payload.records : [];
  state.sourceFiles = Array.isArray(payload.arquivos) ? payload.arquivos : [];
  state.conferencia = payload.conferencia && typeof payload.conferencia === "object" ? payload.conferencia : {};
  state.conferido   = new Set(Array.isArray(payload.conferido) ? payload.conferido : []);
  state.pdvBase     = payload.pdv_base ?? null;
}

const knownTinyClients = new Map();

let _csrfToken = null;
async function getCsrfToken() {
  if (!_csrfToken) {
    try {
      const data = await fetch("/api/csrf-token").then(r => r.json());
      _csrfToken = data.token || "";
    } catch { _csrfToken = ""; }
  }
  return _csrfToken;
}

/**
 * Wrapper de fetch para rotas da API.
 * Trata sessao expirada (401 + session_expired) redirecionando para login
 * em vez de exibir erro de parse de JSON.
 */
function isTinyAuthError(message) {
  if (!message) return false;
  const m = String(message);
  return m.includes("invalid_grant")
      || m.includes("Token is not active")
      || m.includes("Falha ao renovar token")
      || /Erro Tiny [A-Z]+ [a-z]+ \(401\)/.test(m)
      || m.includes("token") && m.includes("401");
}

function promptReauthTiny(contextMsg) {
  const baseMsg = contextMsg ? `${contextMsg}\n\n` : "";
  const ok = confirm(
    `${baseMsg}A autorizacao do Tiny expirou para esta unidade.\n\n` +
    `Clique OK para abrir a tela do Tiny e reautorizar agora.\n` +
    `Apos autorizar, volte para esta aba e tente novamente.`
  );
  if (ok && typeof apiBase !== "undefined" && apiBase) {
    window.open(`${apiBase}/auth`, "_blank");
  }
}

async function apiFetch(path, options = {}) {
  const method = (options.method || "GET").toUpperCase();
  if (method !== "GET") {
    const token = await getCsrfToken();
    options.headers = { ...options.headers, "X-CSRF-Token": token };
  }
  const response = await fetch(path, options);
  const text = await response.text();
  if (!text) {
    throw new Error(`Servidor retornou resposta vazia (HTTP ${response.status}). Tente novamente.`);
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Resposta invalida do servidor (HTTP ${response.status}): ${text.substring(0, 200)}`);
  }
  if (data.session_expired) {
    alert("Sua sessao expirou. Voce sera redirecionado para o login.");
    window.location.href = "/login";
    throw new Error("session_expired");
  }
  return data;
}

const els = {
  fileInput: document.querySelector("#fileInput"),
  loadSampleBtn: document.querySelector("#loadSampleBtn"),
  clearBtn: document.querySelector("#clearBtn"),
  exportBtn: document.querySelector("#exportBtn"),
  recordsBody: document.querySelector("#recordsBody"),
  cashDate: document.querySelector("#cashDate"),
  sourceFile: document.querySelector("#sourceFile"),
  totalSheet: document.querySelector("#totalSheet"),
  totalFa: document.querySelector("#totalFa"),
  totalDetran: document.querySelector("#totalDetran"),
  detranDueDate: document.querySelector("#detranDueDate"),
  totalAv: document.querySelector("#totalAv"),
  pendingCount: document.querySelector("#pendingCount"),
  cashInputTotal: document.querySelector("#cashInputTotal"),
  avDinheiro: document.querySelector("#avDinheiro"),
  avDebito: document.querySelector("#avDebito"),
  avCredito: document.querySelector("#avCredito"),
  avPix: document.querySelector("#avPix"),
  avPendente: document.querySelector("#avPendente"),
  avTotalPanel: document.querySelector("#avTotalPanel"),
  faCountPanel: document.querySelector("#faCountPanel"),
  faTotalPanel: document.querySelector("#faTotalPanel"),
  faDueDate: document.querySelector("#faDueDate"),
  missingClientsPanel: document.querySelector("#missingClientsPanel"),
  tinyReady: document.querySelector("#tinyReady"),
  detranCountPanel: document.querySelector("#detranCountPanel"),
  detranTotalPanel: document.querySelector("#detranTotalPanel"),
  detranMissingPanel: document.querySelector("#detranMissingPanel"),
  detranReady: document.querySelector("#detranReady"),
  detranCard: document.querySelector("#detranCard"),
  grossTotal: document.querySelector("#grossTotal"),
  returnAmount: document.querySelector("#returnAmount"),
  netTotal: document.querySelector("#netTotal"),
  summaryDayMeta: document.querySelector("#summaryDayMeta"),
  avCountMeta: document.querySelector("#avCountMeta"),
  faTotalSummary: document.querySelector("#faTotalSummary"),
  faCountMeta: document.querySelector("#faCountMeta"),
  summaryByService: document.querySelector("#summaryByService"),
  cashDiff: document.querySelector("#cashDiff"),
  closingStatus: document.querySelector("#closingStatus"),
  tableSubtitle: document.querySelector("#tableSubtitle"),
  issuesList: document.querySelector("#issuesList"),
  validateBtn: document.querySelector("#validateBtn"),
  autoMapBtn: document.querySelector("#autoMapBtn"),
  mapClientesBtn: document.querySelector("#mapClientesBtn"),
  previewBtn: document.querySelector("#previewBtn"),
  sendBtn: document.querySelector("#sendBtn"),
  previewModal: document.querySelector("#previewModal"),
  previewSummary: document.querySelector("#previewSummary"),
  previewBody: document.querySelector("#previewBody"),
  confirmModal: document.querySelector("#confirmModal"),
  confirmSummary: document.querySelector("#confirmSummary"),
  confirmBody: document.querySelector("#confirmBody"),
  confirmSendBtn: document.querySelector("#confirmSendBtn")
};

function cleanText(value) {
  return String(value || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

function removeAccents(value) {
  return cleanText(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function normalizeKey(value) {
  return removeAccents(value).toUpperCase().replace(/[^A-Z0-9]+/g, " ").trim();
}

function parseMoney(value) {
  const cleaned = cleanText(value).replace(/[^\d,.-]/g, "");
  if (!cleaned) return 0;
  if (cleaned.includes(",")) {
    return Number(cleaned.replace(/\./g, "").replace(",", "."));
  }
  return Number(cleaned);
}

function isFaturadoFP(fp) { return fp === "FA" || fp === "detran"; }

function fmtCpf(d) {
  if (!d) return "";
  if (d.length === 11) return d.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4");
  if (d.length === 14) return d.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
  return d;
}

function formatMoney(value) {
  return value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function parseDateBr(value) {
  const text = cleanText(value);
  const match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!match) return "";
  const day = match[1].padStart(2, "0");
  const month = match[2].padStart(2, "0");
  return `${match[3]}-${month}-${day}`;
}

function formatDateBr(value) {
  if (!value) return "--/--/----";
  const [year, month, day] = value.split("-");
  return `${day}/${month}/${year}`;
}

function lastDayOfMonth(dateIso) {
  if (!dateIso) return "";
  const [year, month] = dateIso.split("-").map(Number);
  const date = new Date(year, month, 0);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function normalizePlate(value) {
  return removeAccents(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function normalizeService(value) {
  const raw = cleanText(value).toUpperCase();
  const key = normalizeKey(raw);
  for (const [from, to] of Object.entries(serviceAliases)) {
    if (normalizeKey(from) === key) return to;
  }
  return raw;
}

function serviceType(service) {
  const key = normalizeKey(service);
  if (key.includes("TRANSFERENCIA") || key.includes("VISTORIA MOVEL")) return "Transferencia";
  if (key.includes("CAUTELAR")) return "Cautelar";
  if (key.includes("VERIFICACAO")) return "Verificacao";
  return "Servico";
}

function makeRecord(row, index, sourceFile) {
  const [data, modelo, placa, cliente, servico, fp, preco] = row;
  const normalizedService = normalizeService(servico);
  return {
    id: `${sourceFile}-${index}`,
    data: parseDateBr(data),
    modelo: cleanText(modelo).toUpperCase(),
    placa: normalizePlate(placa),
    cliente: cleanText(cliente).toUpperCase(),
    servico: normalizedService,
    tipoServico: serviceType(normalizedService),
    fp: cleanText(fp).toUpperCase(),
    preco: parseMoney(preco),
    origemArquivo: sourceFile,
    linhaOrigem: index + 2,
    avPagamento: "pendente",
    tinyClienteId: knownTinyClients.get(normalizeKey(cliente)) || null,
    ignorar: false
  };
}

function loadSample() {
  state.records = sampleRows.map((row, index) => makeRecord(row, index, "14_04_2026.xls"));
  state.sourceFiles = ["14_04_2026.xls"];
  state.conferencia = {};
  state.conferido = new Set();
  render();
  conferirComPDV();
}

function clearBatch() {
  state.records = [];
  state.sourceFiles = [];
  state.conferencia = {};
  state.conferido = new Set();
  els.fileInput.value = "";
  render();
}

function parseExportedHtml(text, sourceFile) {
  const doc = new DOMParser().parseFromString(text, "text/html");
  const rows = [...doc.querySelectorAll("tr")].map((tr) => [...tr.children].map((cell) => cleanText(cell.textContent)));
  const headerIndex = rows.findIndex((row) => {
    const keys = row.map(normalizeKey);
    return ["DATA", "MODELO", "PLACA", "CLIENTE", "SERVICO", "FP", "PRECO"].every((key) => keys.includes(key));
  });
  if (headerIndex < 0) {
    throw new Error("Nao encontrei as colunas obrigatorias.");
  }

  const headers = rows[headerIndex].map(normalizeKey);
  const column = {
    data: headers.indexOf("DATA"),
    modelo: headers.indexOf("MODELO"),
    placa: headers.indexOf("PLACA"),
    cliente: headers.indexOf("CLIENTE"),
    servico: headers.indexOf("SERVICO"),
    fp: headers.indexOf("FP"),
    preco: headers.indexOf("PRECO")
  };

  return rows.slice(headerIndex + 1).reduce((records, row, rowIndex) => {
    const data = row[column.data] || "";
    if (!/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(data)) return records;
    const parsedRow = [
      row[column.data],
      row[column.modelo],
      row[column.placa],
      row[column.cliente],
      row[column.servico],
      row[column.fp],
      row[column.preco]
    ];
    records.push(makeRecord(parsedRow, rowIndex, sourceFile));
    return records;
  }, []);
}

function recordsForFilter() {
  if (state.filter === "FA") return state.records.filter((record) => record.fp === "FA");
  if (state.filter === "detran") return state.records.filter((record) => record.fp === "detran");
  if (state.filter === "AV") return state.records.filter((record) => record.fp === "AV");
  if (state.filter === "pendencias") return state.records.filter((record) => recordIssues(record).length > 0);
  return state.records;
}

function recordIssues(record) {
  const issues = [];
  // "sem-vinculo" = usuario confirmou que cliente nao existe no Tiny → nao pode enviar
  if (isFaturadoFP(record.fp) && (!record.tinyClienteId || record.tinyClienteId === "sem-vinculo")) issues.push("cliente Tiny");
  if (record.fp === "AV" && record.avPagamento === "pendente") issues.push("pagamento AV");
  if (!record.preco || record.preco <= 0) issues.push("valor");
  // Cruzamento PDV: bloqueia envio ate usuario confirmar divergencias (valor ou FP)
  const conf = state.conferencia[record.id];
  if (conf && !state.conferido.has(record.id)) {
    if (conf.status === "divergencia_valor") issues.push("valor divergente do PDV");
    if (conf.status === "divergencia_fp")    issues.push("FP divergente do PDV");
    if (conf.status === "sem_pdv" && record.fp === "AV") issues.push("sem registro no PDV");
  }
  return issues;
}

function isTinySendable(record) {
  return recordIssues(record).length === 0;
}

function render() {
  renderTable();
  renderSummary();
  renderIssues();
}

function renderTable() {
  const rows = recordsForFilter();
  els.recordsBody.innerHTML = "";
  rows.forEach((record) => {
    const tr = document.createElement("tr");
    const issues = recordIssues(record);
    if (record.pdvExtra) tr.classList.add("row-pdv-extra");
    tr.innerHTML = `
      <td>${formatDateBr(record.data)}</td>
      <td>
        <strong>${escHtml(record.cliente)}</strong>
        <div class="cell-muted">${record.cpf ? fmtCpf(record.cpf) : record.fp === "FA" ? "Faturado" : record.fp === "detran" ? "Taxa DETRAN" : "Particular / caixa"}</div>
      </td>
      <td>
        <strong>${escHtml(record.tipoServico)}</strong>
        <div class="cell-muted">${record.pdvExtra ? `<span class="pdv-origin-badge">&#128242; PDV ${escHtml(record.origemArquivo.replace("PDV ", ""))}</span>` : escHtml(record.origemArquivo)}</div>
      </td>
      <td><span class="placa-tag">${escHtml(record.placa)}</span></td>
      <td>${escHtml(record.servico)}</td>
      <td><span class="fp-chip ${record.fp === "FA" ? "fa" : record.fp === "detran" ? "detran" : "av"}">${record.fp === "detran" ? "DETRAN" : escHtml(record.fp)}</span></td>
      <td>${paymentControl(record)}</td>
      <td class="amount">${formatMoney(record.preco)}</td>
      <td>${tinyControl(record, issues)}</td>
    `;
    els.recordsBody.appendChild(tr);
  });

  els.recordsBody.querySelectorAll("select[data-record]").forEach((select) => {
    select.addEventListener("change", (event) => {
      const record = state.records.find((item) => item.id === event.target.dataset.record);
      if (record) {
        record.avPagamento = event.target.value;
        render();
      }
    });
  });

  els.recordsBody.querySelectorAll("button[data-map-client]").forEach((button) => {
    button.addEventListener("click", (event) => {
      const record = state.records.find((item) => item.id === event.target.dataset.mapClient);
      if (record) {
        // Abre o modal de mapeamento iniciando pelo cliente deste registro
        mapState.queue = [
          record.cliente,
          ...new Set(
            state.records
              .filter((r) => isFaturadoFP(r.fp) && !r.tinyClienteId && r.cliente !== record.cliente)
              .map((r) => r.cliente)
          ),
        ];
        document.querySelector("#mapClientesModal").classList.remove("hidden");
        mapNext();
      }
    });
  });

  els.recordsBody.querySelectorAll("button[data-confirm]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      const id  = event.target.dataset.confirm;
      const rec = state.records.find((r) => r.id === id);
      if (rec && rec.avPagamento === "pendente") {
        alert("Defina a forma de pagamento antes de confirmar.");
        return;
      }
      // Registra divergência antes de confirmar
      if (rec && apiBase) {
        const conf = state.conferencia[id] || {};
        getCsrfToken().then(token => fetch(`${apiBase}/api/divergencias/registrar`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-CSRF-Token": token },
          body: JSON.stringify({
            placa:     rec.placa     || "",
            cliente:   rec.cliente   || "",
            servico:   rec.servico   || "",
            valor:     rec.preco     || 0,
            fp:        rec.avPagamento && rec.avPagamento !== "pendente" ? rec.avPagamento : (rec.fp || ""),
            motivo:    conf.status   || "sem_pdv",
            pdv_valor: conf.pdv_valor ?? null,
            pdv_fp:    conf.pdv_fp   || "",
            arquivo:   (state.sourceFiles || []).join(", "),
          }),
        })).catch(() => {});
      }
      state.conferido.add(id);
      render();
    });
  });

  els.tableSubtitle.textContent = `${rows.length} linha(s) em exibicao`;
}

function paymentControl(record) {
  const conf = state.conferencia[record.id];
  const confirmed = state.conferido.has(record.id);
  const fp_label = { dinheiro: "Dinheiro", debito: "Debito", credito: "Credito", pix: "PIX", faturado: "Faturado" };
  const fmt = v => v != null ? "R$\u00a0" + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "—";

  // FA: normalmente sem controle, mas mostra badge se FP divergente
  if (record.fp !== "AV") {
    if (conf && conf.status === "divergencia_fp") {
      const pdvLabel = `${fp_label[conf.pdv_fp] || conf.pdv_fp} · ${conf.pdv_hora || ""}`;
      const confirmBtn = confirmed
        ? `<span class="pdv-confirmed">confirmado</span>`
        : `<button class="pdv-confirm-btn" data-confirm="${record.id}" type="button">Confirmar assim</button>`;
      return `<div class="pdv-badge pdv-warn">&#x26A0;&#xFE0F; PDV: ${pdvLabel} (planilha: FA) ${confirmBtn}</div>`;
    }
    return `<span style="color:var(--muted)">—</span>`;
  }

  // AV: select de forma de pagamento + badge de conferência
  const options = [
    ["pendente", "A definir"],
    ["dinheiro", "Dinheiro"],
    ["debito", "Debito"],
    ["credito", "Credito"],
    ["pix", "Pix"]
  ];
  const select = `
    <select data-record="${record.id}" aria-label="Pagamento AV">
      ${options.map(([value, label]) => `<option value="${value}" ${record.avPagamento === value ? "selected" : ""}>${label}</option>`).join("")}
    </select>
  `;
  if (!conf) return select;

  if (conf.status === "ok") {
    const label = `${fp_label[conf.pdv_fp] || conf.pdv_fp} · ${conf.pdv_hora}`;
    return select + `<div class="pdv-badge pdv-ok">&#x2705; PDV: ${label}</div>`;
  }
  if (conf.status === "ok_fallback") {
    const label = `${fp_label[conf.pdv_fp] || conf.pdv_fp} · ${conf.pdv_hora || ""}`;
    const srvOriginal = conf.pdv_servico_original ? ` (PDV: ${String(conf.pdv_servico_original).toUpperCase()})` : "";
    return select + `<div class="pdv-badge pdv-ok">&#x2705; PDV: ${label}${srvOriginal}</div>`;
  }
  const confirmBtn = confirmed
    ? `<span class="pdv-confirmed">confirmado</span>`
    : `<button class="pdv-confirm-btn" data-confirm="${record.id}" type="button">Confirmar assim</button>`;
  if (conf.status === "divergencia_fp") {
    const pdvLabel = `${fp_label[conf.pdv_fp] || conf.pdv_fp} · ${conf.pdv_hora || ""}`;
    return select + `<div class="pdv-badge pdv-warn">&#x26A0;&#xFE0F; PDV: ${pdvLabel} (planilha: AV) ${confirmBtn}</div>`;
  }
  if (conf.status === "divergencia_valor") {
    return select + `<div class="pdv-badge pdv-warn">&#x26A0;&#xFE0F; PDV: ${fmt(conf.pdv_valor)} (${fp_label[conf.pdv_fp] || conf.pdv_fp}) · planilha: ${fmt(record.preco)} ${confirmBtn}</div>`;
  }
  // sem_pdv
  return select + `<div class="pdv-badge pdv-err">&#x274C; Sem registro no PDV ${confirmBtn}</div>`;
}

function tinyControl(record, issues) {
  if (isFaturadoFP(record.fp)) {
    if (!record.tinyClienteId || record.tinyClienteId === "sem-vinculo") {
      const label = record.tinyClienteId === "sem-vinculo" ? "Nao encontrado" : "Mapear cliente";
      return `<button class="client-select" type="button" data-map-client="${record.id}">${label}</button>`;
    }
    return `<span class="status ok">Pronto</span>`;
  }
  // AV
  return issues.length
    ? `<span class="status pending">A definir</span>`
    : `<span class="status ok">Pronto</span>`;
}

function renderSummary() {
  const total = sum(state.records);
  const faRecords = state.records.filter((record) => record.fp === "FA");
  const detranRecords = state.records.filter((record) => record.fp === "detran");
  const avRecords = state.records.filter((record) => record.fp === "AV");
  const totalFa = sum(faRecords);
  const totalDetran = sum(detranRecords);
  const totalAv = sum(avRecords);
  const missingClients = faRecords.filter((record) => !record.tinyClienteId).length;
  const detranMissing = detranRecords.filter((record) => !record.tinyClienteId).length;
  const pending = state.records.filter((record) => recordIssues(record).length > 0).length;
  const firstDate = state.records[0]?.data || "";
  const dueDate = firstDate ? lastDayOfMonth(firstDate) : "";
  const returnAmount = parseMoney(els.returnAmount.value);
  const netTotal = total - returnAmount;

  // Entradas físicas: PDV base + AV confirmadas como sem_pdv (para refletir fechamento final)
  if (state.pdvBase) {
    const eff = { ...state.pdvBase };
    for (const r of avRecords) {
      if (!state.conferido.has(r.id)) continue;
      const conf = state.conferencia[r.id];
      if (!conf || conf.status !== "sem_pdv") continue;
      const fp = r.avPagamento;
      if (fp && eff[fp] !== undefined) eff[fp] += Number(r.preco) || 0;
    }
    ["dinheiro", "debito", "credito", "pix"].forEach((k) => {
      const v = eff[k] || 0;
      const disp = document.querySelector(`.pdv-val[data-pdv-cash="${k}"]`);
      const inp  = document.querySelector(`.money-input[data-cash="${k}"]`);
      if (disp) {
        disp.textContent = v > 0 ? formatMoney(v) : "—";
        disp.classList.toggle("is-empty", !(v > 0));
      }
      if (inp) inp.value = v > 0 ? v.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "";
    });
  }
  const cashInputs = [...document.querySelectorAll("[data-cash]")].reduce((acc, input) => acc + parseMoney(input.value), 0);

  els.cashDate.textContent = formatDateBr(firstDate);
  els.sourceFile.textContent = state.sourceFiles.length ? `${state.sourceFiles.length} arquivo(s)` : "Nenhum arquivo";
  els.totalSheet.textContent = formatMoney(total);
  els.totalFa.textContent = formatMoney(totalFa);
  els.totalDetran.textContent = formatMoney(totalDetran);
  if (els.detranDueDate) els.detranDueDate.textContent = formatDateBr(dueDate);
  els.totalAv.textContent = formatMoney(totalAv);
  els.pendingCount.textContent = String(pending);
  els.cashInputTotal.textContent = formatMoney(cashInputs);
  els.grossTotal.textContent = formatMoney(total);
  els.netTotal.textContent = formatMoney(netTotal);

  // Resumo v2: contagens e breakdown por categoria de serviço
  const activeRecords = state.records.filter((r) => !r.ignorar);
  const plural = (n, s, p) => `${n} ${n === 1 ? s : p}`;
  if (els.summaryDayMeta) els.summaryDayMeta.textContent = plural(activeRecords.length, "lançamento", "lançamentos");
  if (els.avCountMeta)    els.avCountMeta.textContent    = plural(avRecords.length, "lançamento", "lançamentos");
  if (els.faCountMeta)    els.faCountMeta.textContent    = plural(faRecords.length, "lançamento", "lançamentos");
  if (els.faTotalSummary) els.faTotalSummary.textContent = formatMoney(totalFa);

  if (els.summaryByService) {
    const grouped = {};
    for (const r of activeRecords) {
      const key = r.tipoServico || "Servico";
      if (!grouped[key]) grouped[key] = { total: 0, qty: 0 };
      grouped[key].total += Number(r.preco) || 0;
      grouped[key].qty += 1;
    }
    const displayName = (k) => (k === "Verificacao" ? "Verificação" : k === "Transferencia" ? "Transferência" : k);
    const order = ["Cautelar", "Verificacao", "Transferencia", "Servico"];
    const keys = Object.keys(grouped).sort((a, b) => {
      const ia = order.indexOf(a); const ib = order.indexOf(b);
      return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
    });
    els.summaryByService.innerHTML = keys.length === 0
      ? `<div class="svc-line"><span style="color:var(--t5);font-weight:500">Nenhum lançamento</span></div>`
      : keys.map((k) => `
          <div class="svc-line">
            <span>${displayName(k)}</span>
            <div class="svc-line-right">
              <span class="svc-value">${formatMoney(grouped[k].total)}</span>
              <span class="svc-qty">${grouped[k].qty}</span>
            </div>
          </div>`).join("");
  }
  els.cashDiff.textContent = formatMoney(cashInputs - totalAv);
  els.closingStatus.textContent = pending > 0 ? "Pendente" : Math.abs(cashInputs - totalAv) < 0.01 ? "Conferido" : "Conferir";

  els.avDinheiro.textContent = formatMoney(sum(avRecords.filter((record) => record.avPagamento === "dinheiro")));
  els.avDebito.textContent = formatMoney(sum(avRecords.filter((record) => record.avPagamento === "debito")));
  els.avCredito.textContent = formatMoney(sum(avRecords.filter((record) => record.avPagamento === "credito")));
  els.avPix.textContent = formatMoney(sum(avRecords.filter((record) => record.avPagamento === "pix")));
  els.avPendente.textContent = formatMoney(sum(avRecords.filter((record) => record.avPagamento === "pendente")));
  els.avTotalPanel.textContent = formatMoney(totalAv);

  els.faCountPanel.textContent = String(faRecords.length);
  els.faTotalPanel.textContent = formatMoney(totalFa);
  els.faDueDate.textContent = formatDateBr(dueDate);
  els.missingClientsPanel.textContent = String(missingClients);
  els.tinyReady.textContent = missingClients === 0 && faRecords.length > 0 ? "Pronto" : "Pendente";
  if (els.detranCard) els.detranCard.style.display = detranRecords.length ? "" : "none";
  if (els.detranCountPanel) els.detranCountPanel.textContent = String(detranRecords.length);
  if (els.detranTotalPanel) els.detranTotalPanel.textContent = formatMoney(totalDetran);
  if (els.detranMissingPanel) els.detranMissingPanel.textContent = String(detranMissing);
  if (els.detranReady) els.detranReady.textContent = detranMissing === 0 && detranRecords.length > 0 ? "Pronto" : "Pendente";
  const sendableCount = state.records.filter(isTinySendable).length;
  els.sendBtn.disabled = sendableCount === 0;
  els.previewBtn.disabled = sendableCount === 0;
  const unmappedFaturado = state.records.filter((r) => isFaturadoFP(r.fp) && !r.tinyClienteId);
  els.autoMapBtn.hidden = unmappedFaturado.length === 0;
  els.mapClientesBtn.hidden = unmappedFaturado.length === 0;
  autosaveLocal();
}

function renderIssues() {
  const issues = [];
  const faMissing = state.records.filter((r) => isFaturadoFP(r.fp) && !r.tinyClienteId);
  const avPending = state.records.filter((r) => r.fp === "AV" && r.avPagamento === "pendente");
  const pdvDivergFP = state.records.filter((r) => {
    const conf = state.conferencia[r.id];
    return conf && conf.status === "divergencia_fp" && !state.conferido.has(r.id);
  });
  const pdvDivergVal = state.records.filter((r) => {
    const conf = state.conferencia[r.id];
    return conf && conf.status === "divergencia_valor" && !state.conferido.has(r.id);
  });
  const pdvMissing = state.records.filter((r) => {
    const conf = state.conferencia[r.id];
    return r.fp === "AV" && conf && conf.status === "sem_pdv" && !state.conferido.has(r.id);
  });
  if (faMissing.length) issues.push(`${faMissing.length} cliente(s) FA sem vinculo Tiny.`);
  if (avPending.length) issues.push(`${avPending.length} item(ns) AV sem forma de pagamento.`);
  if (pdvDivergFP.length) issues.push(`${pdvDivergFP.length} item(ns) com FP divergente — confirmar ou corrigir.`);
  if (pdvDivergVal.length) issues.push(`${pdvDivergVal.length} item(ns) com valor divergente do PDV.`);
  if (pdvMissing.length) issues.push(`${pdvMissing.length} item(ns) AV sem registro no PDV de hoje.`);
  if (!state.records.length) issues.push("Nenhuma planilha carregada.");
  if (!issues.length) issues.push("Lote pronto para conferencia final.");
  els.issuesList.innerHTML = issues.map((issue) => `<li>${issue}</li>`).join("");

  // Painel de correções para o administrativo
  renderFpCorrections(pdvDivergFP);
}

function renderFpCorrections(divergentes) {
  const panel = document.getElementById("fpCorrectionsPanel");
  if (!panel) return;
  if (!divergentes.length) { panel.hidden = true; return; }
  panel.hidden = false;
  const fp_label = { dinheiro: "Dinheiro", debito: "Debito", credito: "Credito", pix: "PIX", faturado: "Faturado" };
  const rows = divergentes.map((r) => {
    const conf = state.conferencia[r.id];
    const pdvFp = fp_label[conf.pdv_fp] || conf.pdv_fp || "—";
    const pdvFpCat = conf.pdv_fp === "faturado" ? "FA" : "AV";
    return `<tr>
      <td>${escHtml(r.placa)}</td>
      <td>${escHtml(r.cliente)}</td>
      <td>${escHtml(r.servico)}</td>
      <td><span class="corr-tag">${escHtml(r.fp)}</span> → <span class="corr-tag corr-pdv">${escHtml(pdvFpCat)} (${escHtml(pdvFp)})</span></td>
      <td style="color:var(--t4);font-size:12px">${escHtml(conf.pdv_hora || "—")}</td>
    </tr>`;
  }).join("");
  document.getElementById("fpCorrBody").innerHTML = rows;
}

async function conferirComPDV() {
  if (!apiBase) return;
  const temPlanilha = state.records.some((r) => !r.pdvExtra);
  const planilhaRecords = state.records
    .filter((r) => !r.pdvExtra)
    .map((r) => ({ id: r.id, placa: r.placa, servico: r.servico, preco: r.preco, fp: r.fp, data: r.data }));
  try {
    const result = await apiFetch(`${apiBase}/api/caixa/conferir`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ records: planilhaRecords }),
    });
    if (!result.success) return;
    state.conferencia = result.conferencia;

    // Auto-preenche avPagamento para registros confirmados no PDV
    // ("ok" = match por placa+servico, "ok_fallback" = match por placa+valor
    // quando o servico diverge entre planilha e PDV).
    for (const [id, conf] of Object.entries(result.conferencia)) {
      const matched = conf.status === "ok" || conf.status === "ok_fallback";
      if (matched && conf.pdv_fp) {
        const rec = state.records.find((r) => r.id === id);
        if (rec && rec.avPagamento === "pendente") {
          rec.avPagamento = conf.pdv_fp;
        }
      }
    }

    // Injeta lançamentos do PDV que não têm correspondência na planilha
    // (serviços que nunca vêm no Excel: PESQUISA AVULSA, BAIXA PERMANENTE etc.)
    // Alterado a pedido: permite visualizar AV mesmo sem planilha
    const extras = (result.pdv_sem_planilha || []);
    // Remove extras anteriores para não duplicar em chamadas subsequentes
    state.records = state.records.filter((r) => !r.pdvExtra);
    for (const lc of extras) {
      const hoje = lc.timestamp ? lc.timestamp.slice(0, 10) : new Date().toISOString().slice(0, 10);
      state.records.push({
        id: `pdv-${lc.pdv_id}`,
        data: hoje,
        modelo: "",
        placa: lc.placa || "",
        cliente: (lc.cliente || "").toUpperCase(),
        cpf: lc.cpf || "",
        servico: (lc.servico || "").toUpperCase(),
        tipoServico: serviceType(lc.servico || ""),
        fp: lc.fp === "faturado" ? "FA" : "AV",
        preco: Number(lc.valor) || 0,
        origemArquivo: `PDV ${lc.hora || ""}`.trim(),
        linhaOrigem: 0,
        avPagamento: lc.fp === "faturado" ? "pendente" : (lc.fp || "pendente"),
        tinyClienteId: knownTinyClients.get(normalizeKey(lc.cliente || "")) || null,
        ignorar: false,
        pdvExtra: true,   // marca para identificação visual e limpeza
      });
    }

    render();
  } catch (e) {
    // conferencia e opcional — falha silenciosa
  }
}

function sum(records) {
  return records.reduce((acc, record) => acc + record.preco, 0);
}

function exportConference() {
  const payload = {
    arquivos: state.sourceFiles,
    geradoEm: new Date().toISOString(),
    totais: {
      planilha: sum(state.records),
      fa: sum(state.records.filter((record) => record.fp === "FA")),
      detran: sum(state.records.filter((record) => record.fp === "detran")),
      av: sum(state.records.filter((record) => record.fp === "AV"))
    },
    registros: state.records
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = "conferencia_frente_caixa.json";
  link.click();
  URL.revokeObjectURL(link.href);
}

function readFileText(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Nao foi possivel ler o arquivo."));
    reader.readAsText(file, "windows-1252");
  });
}

els.fileInput.addEventListener("change", async (event) => {
  const files = [...event.target.files];
  if (!files.length) return;
  const imported = [];
  const errors = [];
  const skipped = [];

  // Chave de deduplicação por conteúdo do registro
  const recordKey = (r) => `${r.data}|${r.placa}|${r.servico}|${r.valor}|${r.fp}`;
  const existingKeys = new Set(state.records.map(recordKey));

  for (const file of files) {
    // Bloqueia mesmo arquivo importado duas vezes
    if (state.sourceFiles.includes(file.name)) {
      skipped.push(file.name);
      continue;
    }
    try {
      const text = await readFileText(file);
      const records = parseExportedHtml(text, file.name);
      if (!records.length) throw new Error("Nenhum registro valido encontrado.");

      // Filtra registros duplicados por conteúdo
      const novos = records.filter(r => !existingKeys.has(recordKey(r)));
      const duplicados = records.length - novos.length;
      novos.forEach(r => existingKeys.add(recordKey(r)));
      imported.push(...novos);
      state.sourceFiles.push(file.name);

      if (duplicados > 0) {
        skipped.push(`${file.name}: ${duplicados} registro(s) duplicado(s) ignorado(s)`);
      }
    } catch (error) {
      errors.push(`${file.name}: ${error.message}`);
    }
  }

  if (skipped.length) {
    alert("⚠️ Arquivo(s) já importado(s) ou com duplicatas:\n\n" + skipped.join("\n"));
  }

  state.records = [...state.records, ...imported];
  state.conferencia = {};
  state.conferido = new Set();
  render();
  conferirComPDV();
  if (imported.length) {
    saveSnapshotAsync("import").catch(() => {});
  }
  if (errors.length) {
    alert(errors.join("\n"));
  }
});

if (els.loadSampleBtn) els.loadSampleBtn.addEventListener("click", loadSample);
els.clearBtn.addEventListener("click", clearBatch);
els.exportBtn.addEventListener("click", exportConference);
els.validateBtn.addEventListener("click", render);
// Dados do lote aguardando confirmacao (preenchido ao abrir o modal de confirmacao)
let _pendingToSend = [];

els.sendBtn.addEventListener("click", async () => {
  const toSend = state.records.filter((r) => isTinySendable(r));
  if (!toSend.length) return;

  els.sendBtn.disabled = true;
  els.sendBtn.textContent = "Carregando...";

  try {
    const result = await apiFetch(`${apiBase}/api/preview`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: state.sourceFiles[0] || "manual_ui", records: toSend }),
    });
    if (!result.success) { alert(`Erro: ${result.error}`); return; }

    _pendingToSend = toSend;
    renderPreviewRows(result.previews, els.confirmBody, els.confirmSummary);
    // Guarda previews para exportacao
    els.confirmModal._previews = result.previews;
    openConfirmModal();
  } catch (err) {
    if (err.message !== "session_expired") alert(`Erro de conexao: ${err.message}`);
  } finally {
    els.sendBtn.disabled = false;
    els.sendBtn.textContent = "Enviar para Tiny";
  }
});

document.querySelector("#confirmModalClose").addEventListener("click", closeConfirmModal);
document.querySelector("#confirmModalCancel").addEventListener("click", closeConfirmModal);
els.confirmModal.addEventListener("click", (e) => { if (e.target === els.confirmModal) closeConfirmModal(); });

document.querySelector("#confirmExportPdf").addEventListener("click", () => { window.print(); });

document.querySelector("#confirmExportCsv").addEventListener("click", () => {
  const date = new Date().toISOString().slice(0, 10);
  exportPreviewCsv(els.confirmModal._previews || [], `lancamentos_${date}.csv`);
});

els.confirmSendBtn.addEventListener("click", async () => {
  if (!_pendingToSend.length) return;

  els.confirmSendBtn.disabled = true;
  const BATCH_SIZE = 20; // servidor processa em paralelo (5 threads); Gunicorn timeout=120s
  const source = state.sourceFiles[0] || "manual_ui";
  const total = { enviados: [], pulados: [], falhas: [] };
  let tokenError = false;

  try {
    const batches = [];
    for (let i = 0; i < _pendingToSend.length; i += BATCH_SIZE) {
      batches.push(_pendingToSend.slice(i, i + BATCH_SIZE));
    }

    for (let i = 0; i < batches.length; i++) {
      els.confirmSendBtn.textContent = `Enviando lote ${i + 1}/${batches.length}...`;
      const result = await apiFetch(`${apiBase}/api/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source, records: batches[i] }),
      });

      if (!result.success) {
        const errMsg = result.error || "";
        const tokenErr = errMsg.includes("token") || errMsg.includes("Token") || errMsg.includes("401");
        const suffix = tokenErr
          ? `\n\n⚠️ Token invalido ou expirado. Reautorize em:\n${window.location.origin}${apiBase}/auth`
          : "";
        alert(`Erro no servidor (lote ${i + 1}): ${errMsg}${suffix}`);
        return;
      }

      const s = result.summary;
      total.enviados.push(...(s.enviados || []));
      total.pulados.push(...(s.pulados || []));
      total.falhas.push(...(s.falhas || []));

      if (s.falhas && s.falhas.some((f) => f.erro && (f.erro.includes("token") || f.erro.includes("Token") || f.erro.includes("401")))) {
        tokenError = true;
      }
    }

    closeConfirmModal();
    _pendingToSend = [];

    let msg = `Enviados com sucesso: ${total.enviados.length}`;
    if (total.pulados.length) msg += `\nPulados (ja existiam): ${total.pulados.length}`;
    if (total.falhas.length) {
      msg += `\nFalhas: ${total.falhas.length}`;
      const detalhes = total.falhas.map((f) => `  • ${f.cliente}: ${f.erro}`).join("\n");
      msg += `\n\nDetalhes das falhas:\n${detalhes}`;
    }
    if (tokenError) {
      promptReauthTiny(msg);
    } else {
      alert(msg);
    }

    if (total.falhas.length === 0 && confirm("Deseja limpar o lote atual?")) {
      clearBatch();
    }
  } catch (err) {
    if (err.message !== "session_expired") alert(`Erro de conexao: ${err.message}`);
  } finally {
    els.confirmSendBtn.disabled = false;
    els.confirmSendBtn.textContent = "Confirmar e Enviar";
  }
});

document.querySelector("#clearImportedBtn").addEventListener("click", async () => {
  if (!confirm("Limpar o historico local de envios?\n\nIsso permite reenviar registros que ja foram enviados anteriormente (por exemplo, apos excluir manualmente do Tiny).\n\nNao remove nada do Tiny, apenas reinicia o controle local.")) return;
  try {
    const d = await apiFetch(`${apiBase}/api/clear-imported`, { method: "POST" });
    alert(d.success ? d.message : `Erro: ${d.error}`);
  } catch (e) {
    if (e.message !== "session_expired") alert(`Erro de conexao: ${e.message}`);
  }
});

document.querySelectorAll(".filter").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".filter").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    state.filter = button.dataset.filter;
    renderTable();
  });
});

document.querySelectorAll(".money-input").forEach((input) => {
  input.addEventListener("input", renderSummary);
});

// --- Helpers compartilhados pelos dois modais ---

function renderPreviewRows(previews, tbodyEl, summaryEl) {
  summaryEl.innerHTML = `
    <span class="badge new">${previews.filter((p) => !p.jaEnviado).length} novo(s)</span>
    ${previews.some((p) => p.jaEnviado) ? `<span class="badge dup">${previews.filter((p) => p.jaEnviado).length} ja enviado(s) — sera pulado</span>` : ""}
    <span class="badge" style="background:var(--calc)">${previews.length} total</span>
  `;
  tbodyEl.innerHTML = previews.map((p) => `
    <tr class="${p.jaEnviado ? "duplicata" : ""}">
      <td><strong>${escHtml(p.cliente)}</strong></td>
      <td><span class="status ${p.fp === "FA" ? "ok" : "pending"}">${escHtml(p.fp)}</span></td>
      <td>${escHtml(p.avPagamento || "—")}</td>
      <td class="amount">${formatMoney(p.valor)}</td>
      <td>${formatDateBr(p.dataVencimento)}</td>
      <td>${escHtml(p.formaRecebimento)}</td>
      <td style="font-size:12px;color:var(--muted)">${escHtml(p.numeroDocumento)}</td>
      <td style="font-size:11px;color:var(--muted)">${escHtml(p.servico || "—")}</td>
      <td>${p.jaEnviado ? '<span class="status pending">Ja enviado</span>' : '<span class="status ok">Novo</span>'}</td>
    </tr>
  `).join("");
}

function exportPreviewCsv(previews, filename) {
  const header = ["Cliente", "FP", "Pagamento", "Valor", "Vencimento", "Forma Recebimento", "N Documento", "Status"];
  const rows = previews.map((p) => [
    p.cliente,
    p.fp,
    p.avPagamento || "",
    p.valor.toFixed(2).replace(".", ","),
    formatDateBr(p.dataVencimento),
    p.formaRecebimento,
    p.numeroDocumento,
    p.jaEnviado ? "Ja enviado" : "Novo",
  ]);
  const csv = [header, ...rows].map((r) => r.map((v) => `"${String(v).replace(/"/g, '""')}"`).join(";")).join("\r\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = filename;
  link.click();
  URL.revokeObjectURL(link.href);
}

// --- Modal de simulacao (informativo) ---

function openPreviewModal() { els.previewModal.classList.remove("hidden"); }
function closePreviewModal() { els.previewModal.classList.add("hidden"); }

// --- Modal de confirmacao de envio ---

function openConfirmModal() { els.confirmModal.classList.remove("hidden"); }
function closeConfirmModal() { els.confirmModal.classList.add("hidden"); }

document.querySelector("#previewModalClose").addEventListener("click", closePreviewModal);
document.querySelector("#previewModalCloseBtn").addEventListener("click", closePreviewModal);
els.previewModal.addEventListener("click", (e) => { if (e.target === els.previewModal) closePreviewModal(); });

els.previewBtn.addEventListener("click", async () => {
  const toPreview = state.records.filter((r) => isTinySendable(r));
  if (!toPreview.length) return;

  els.previewBtn.disabled = true;
  els.previewBtn.textContent = "Simulando...";

  try {
    const result = await apiFetch(`${apiBase}/api/preview`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: state.sourceFiles[0] || "manual_ui", records: toPreview }),
    });
    if (!result.success) { alert(`Erro: ${result.error}`); return; }

    renderPreviewRows(result.previews, els.previewBody, els.previewSummary);
    openPreviewModal();
  } catch (err) {
    if (err.message !== "session_expired") alert(`Erro de conexao: ${err.message}`);
  } finally {
    els.previewBtn.disabled = false;
    els.previewBtn.textContent = "Simular envio";
  }
});

// --- Modal de mapeamento de clientes FA ---

const mapState = {
  queue: [],       // clientes FA ainda nao mapeados
  current: null,   // cliente sendo processado agora
};

function openMapClientesModal() {
  mapState.queue = [...new Set(
    state.records
      .filter((r) => isFaturadoFP(r.fp) && !r.tinyClienteId)
      .map((r) => r.cliente)
  )];
  if (!mapState.queue.length) return;
  document.querySelector("#mapClientesModal").classList.remove("hidden");
  mapNext();
}

function closeMapClientesModal() {
  document.querySelector("#mapClientesModal").classList.add("hidden");
  document.querySelector("#mapBuscarBox").classList.add("hidden");
  document.querySelector("#mapBuscarInput").value = "";
}

function mapNext() {
  if (!mapState.queue.length) {
    closeMapClientesModal();
    render();
    return;
  }
  mapState.current = mapState.queue.shift();
  const total = state.records.filter((r) => isFaturadoFP(r.fp) && !r.tinyClienteId).length;
  document.querySelector("#mapProgress").textContent =
    `${mapState.queue.length + 1} de ${total + mapState.queue.length} cliente(s) nao mapeado(s)`;
  document.querySelector("#mapClienteAtual").innerHTML =
    `${escHtml(mapState.current)}<span>Nome como aparece na planilha</span>`;
  document.querySelector("#mapBuscarBox").classList.add("hidden");
  document.querySelector("#mapBuscarInput").value = "";
  mapBuscar(mapState.current);
}

async function mapBuscar(nome) {
  const container = document.querySelector("#mapCandidatos");
  container.innerHTML = `<p class="map-empty">Buscando no Tiny...</p>`;
  try {
    const data = await apiFetch(`${apiBase}/api/suggest-clients`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nome }),
    });
    if (!data.success) throw new Error(data.error);
    renderCandidatos(data.candidates);
  } catch (err) {
    const isTokenErr = err.message && (err.message.includes("token") || err.message.includes("Token") || err.message.includes("401"));
    const authLink = isTokenErr
      ? ` <a href="${apiBase}/auth" target="_blank" style="color:#c0392b;font-weight:bold">Clique aqui para autorizar o Tiny</a>`
      : "";
    container.innerHTML = `<p class="map-empty">Erro ao buscar: ${escHtml(err.message)}${authLink}</p>`;
  }
}

function renderCandidatos(candidates) {
  const container = document.querySelector("#mapCandidatos");
  if (!candidates.length) {
    container.innerHTML = `<p class="map-empty">Nenhum candidato encontrado. Tente buscar por outro nome.</p>`;
    return;
  }
  container.innerHTML = `<div class="candidate-list">${candidates.map((c) => {
    const pct = Math.round(c.score * 100);
    const high = pct >= 70;
    return `
      <div class="candidate-card">
        <div class="candidate-info">
          <strong>${escHtml(c.nome)}</strong>
          ${c.fantasia ? `<span>${escHtml(c.fantasia)}</span>` : ""}
        </div>
        <span class="candidate-score ${high ? "high" : ""}">${pct}%</span>
        <button class="candidate-confirm" type="button"
          data-id="${escHtml(c.id)}" data-nome="${escHtml(c.nome)}">Confirmar</button>
      </div>`;
  }).join("")}</div>`;

  container.querySelectorAll(".candidate-confirm").forEach((btn) => {
    btn.addEventListener("click", async () => {
      await mapConfirmar(mapState.current, Number(btn.dataset.id));
    });
  });
}

async function mapConfirmar(clienteNome, tinyId) {
  try {
    const data = await apiFetch(`${apiBase}/api/map-client`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clienteNome, tinyId }),
    });
    if (!data.success) throw new Error(data.error);

    // Atualiza todos os registros com esse cliente em memoria
    state.records.forEach((r) => {
      if (r.cliente === clienteNome) r.tinyClienteId = `tiny-${tinyId}`;
    });
    knownTinyClients.set(normalizeKey(clienteNome), tinyId);
    mapNext();
  } catch (err) {
    alert(`Erro ao salvar: ${err.message}`);
  }
}

async function autoMapClientes() {
  const unmapped = [...new Set(
    state.records.filter((r) => r.fp === "FA" && !r.tinyClienteId).map((r) => r.cliente)
  )];
  if (!unmapped.length) return;

  els.autoMapBtn.disabled = true;
  els.autoMapBtn.textContent = "Mapeando...";

  try {
    const data = await apiFetch(`${apiBase}/api/auto-map-clients`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ clientes: unmapped, threshold: 0.90 }),
    });
    if (!data.success) throw new Error(data.error);

    // Aplica os mapeamentos automaticos na memoria
    for (const m of data.mapped) {
      state.records.forEach((r) => {
        if (r.cliente === m.clienteNome) r.tinyClienteId = `tiny-${m.tinyId}`;
      });
      knownTinyClients.set(normalizeKey(m.clienteNome), m.tinyId);
    }

    const msg = data.mapped.length > 0
      ? `${data.mapped.length} cliente(s) mapeados automaticamente.\n` +
        data.mapped.map((m) => `  ✓ ${m.clienteNome} → ${m.tinyNome} (${Math.round(m.score * 100)}%)`).join("\n")
      : "Nenhum cliente com confiança ≥90% encontrado.";

    alert(msg + (data.needs_review.length ? `\n\n${data.needs_review.length} cliente(s) precisam de mapeamento manual.` : ""));

    render();

    // Abre o modal manual apenas para os que sobraram
    if (data.needs_review.length) {
      openMapClientesModal();
    }
  } catch (err) {
    if (isTinyAuthError(err.message)) {
      promptReauthTiny("Erro no auto-mapeamento.");
    } else {
      alert(`Erro no auto-mapeamento: ${err.message}`);
    }
  } finally {
    els.autoMapBtn.disabled = false;
    els.autoMapBtn.textContent = "Auto-mapear clientes (≥90%)";
  }
}

els.autoMapBtn.addEventListener("click", autoMapClientes);
els.mapClientesBtn.addEventListener("click", openMapClientesModal);
document.querySelector("#mapClientesClose").addEventListener("click", closeMapClientesModal);
document.querySelector("#mapPularBtn").addEventListener("click", mapNext);
document.querySelector("#mapNaoExisteBtn").addEventListener("click", async () => {
  // Marca como "sem vinculo Tiny mas confirmado" para nao bloquear o envio
  state.records.forEach((r) => {
    if (r.cliente === mapState.current) r.tinyClienteId = "sem-vinculo";
  });
  mapNext();
});
document.querySelector("#mapBuscarOutroBtn").addEventListener("click", () => {
  const box = document.querySelector("#mapBuscarBox");
  box.classList.toggle("hidden");
  if (!box.classList.contains("hidden")) document.querySelector("#mapBuscarInput").focus();
});
document.querySelector("#mapBuscarBtn").addEventListener("click", () => {
  const termo = document.querySelector("#mapBuscarInput").value.trim();
  if (termo) mapBuscar(termo);
});
document.querySelector("#mapBuscarInput").addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    const termo = e.target.value.trim();
    if (termo) mapBuscar(termo);
  }
});

// --- Carrega nome da unidade do servidor e atualiza cabecalho, titulo e logout
fetch(`${apiBase}/api/info`)
  .then((r) => r.json())
  .then((info) => {
    if (info.unidade) {
      document.querySelector("#unidadeLabel").textContent = `Unidade ${info.unidade} — Fechamento diario`;
      document.title = `Frente de Caixa — ${info.unidade}`;
    }
    // Mostra botao de sair quando rodando no servidor (com login)
    if (apiBase) {
      const logoutLink = document.getElementById("logoutLink");
      if (logoutLink) logoutLink.style.display = "";
      const linkCaixa = document.getElementById("linkCaixa");
      if (linkCaixa) linkCaixa.href = `${apiBase}/caixa`;
    }
  })
  .catch(() => {});

// Carrega faturados do PDV na abertura da página (mesmo sem planilha importada)
if (apiBase) conferirComPDV();

// Em modo local (sem Railway) carrega exemplo para facilitar testes
if (!apiBase) loadSample();
