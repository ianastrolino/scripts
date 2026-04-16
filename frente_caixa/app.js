const serviceAliases = {
  "LAUDO DE VERIFICACA": "LAUDO DE VERIFICACAO",
  "LAUDO DE VERIFICA": "LAUDO DE VERIFICACAO",
  "LAUDO CAUTELAR VERI": "LAUDO CAUTELAR VERIFICACAO",
  "LAUDO CAUTELAR VE": "LAUDO CAUTELAR VERIFICACAO",
  "CAUTELAR COM ANALIS": "CAUTELAR COM ANALISE",
  "CAUTELAR COM ANAL": "CAUTELAR COM ANALISE",
  "LAUDO DE TRANSFEREN": "LAUDO DE TRANSFERENCIA"
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

const state = {
  records: [],
  sourceFiles: [],
  filter: "todos"
};

const knownTinyClients = new Map();

// Detecta prefixo /u/<unidade> quando rodando no servidor Railway
const _pathMatch = window.location.pathname.match(/^(\/u\/[^/]+)/);
const apiBase = _pathMatch ? _pathMatch[1] : "";

/**
 * Wrapper de fetch para rotas da API.
 * Trata sessao expirada (401 + session_expired) redirecionando para login
 * em vez de exibir erro de parse de JSON.
 */
async function apiFetch(path, options = {}) {
  const response = await fetch(path, options);
  const data = await response.json();
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
  grossTotal: document.querySelector("#grossTotal"),
  returnAmount: document.querySelector("#returnAmount"),
  netTotal: document.querySelector("#netTotal"),
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
  render();
}

function clearBatch() {
  state.records = [];
  state.sourceFiles = [];
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
  if (state.filter === "AV") return state.records.filter((record) => record.fp === "AV");
  if (state.filter === "pendencias") return state.records.filter((record) => recordIssues(record).length > 0);
  return state.records;
}

function recordIssues(record) {
  const issues = [];
  if (record.fp === "FA" && !record.tinyClienteId) issues.push("cliente Tiny");
  if (record.fp === "AV" && record.avPagamento === "pendente") issues.push("pagamento AV");
  if (!record.preco || record.preco <= 0) issues.push("valor");
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
    tr.innerHTML = `
      <td>${formatDateBr(record.data)}</td>
      <td>
        <strong>${record.cliente}</strong>
        <div class="cell-muted">${record.fp === "FA" ? "Faturado" : "Particular / caixa"}</div>
      </td>
      <td>
        <strong>${record.tipoServico}</strong>
        <div class="cell-muted">${record.origemArquivo}</div>
      </td>
      <td>${record.placa}</td>
      <td>${record.servico}</td>
      <td><span class="status ${record.fp === "FA" ? "ok" : "pending"}">${record.fp}</span></td>
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
              .filter((r) => r.fp === "FA" && !r.tinyClienteId && r.cliente !== record.cliente)
              .map((r) => r.cliente)
          ),
        ];
        document.querySelector("#mapClientesModal").classList.remove("hidden");
        mapNext();
      }
    });
  });

  els.tableSubtitle.textContent = `${rows.length} linha(s) em exibicao`;
}

function paymentControl(record) {
  if (record.fp !== "AV") return `<span style="color:var(--muted)">—</span>`;
  const options = [
    ["pendente", "A definir"],
    ["dinheiro", "Dinheiro"],
    ["debito", "Debito"],
    ["credito", "Credito"],
    ["pix", "Pix"]
  ];
  return `
    <select data-record="${record.id}" aria-label="Pagamento AV">
      ${options.map(([value, label]) => `<option value="${value}" ${record.avPagamento === value ? "selected" : ""}>${label}</option>`).join("")}
    </select>
  `;
}

function tinyControl(record, issues) {
  if (record.fp === "FA") {
    if (!record.tinyClienteId) {
      return `<button class="client-select" type="button" data-map-client="${record.id}">Mapear cliente</button>`;
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
  const avRecords = state.records.filter((record) => record.fp === "AV");
  const totalFa = sum(faRecords);
  const totalAv = sum(avRecords);
  const missingClients = faRecords.filter((record) => !record.tinyClienteId).length;
  const pending = state.records.filter((record) => recordIssues(record).length > 0).length;
  const firstDate = state.records[0]?.data || "";
  const dueDate = firstDate ? lastDayOfMonth(firstDate) : "";
  const cashInputs = [...document.querySelectorAll("[data-cash]")].reduce((acc, input) => acc + parseMoney(input.value), 0);
  const returnAmount = parseMoney(els.returnAmount.value);
  const netTotal = total - returnAmount;

  els.cashDate.textContent = formatDateBr(firstDate);
  els.sourceFile.textContent = state.sourceFiles.length ? `${state.sourceFiles.length} arquivo(s)` : "Nenhum arquivo";
  els.totalSheet.textContent = formatMoney(total);
  els.totalFa.textContent = formatMoney(totalFa);
  els.totalAv.textContent = formatMoney(totalAv);
  els.pendingCount.textContent = String(pending);
  els.cashInputTotal.textContent = formatMoney(cashInputs);
  els.grossTotal.textContent = formatMoney(total);
  els.netTotal.textContent = formatMoney(netTotal);
  els.cashDiff.textContent = formatMoney(cashInputs - totalAv);
  els.closingStatus.textContent = Math.abs(cashInputs - totalAv) < 0.01 ? "Conferido" : "Conferir";

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
  const sendableCount = state.records.filter(isTinySendable).length;
  els.sendBtn.disabled = sendableCount === 0;
  els.previewBtn.disabled = sendableCount === 0;
  const unmappedFa = state.records.filter((r) => r.fp === "FA" && !r.tinyClienteId);
  els.autoMapBtn.hidden = unmappedFa.length === 0;
  els.mapClientesBtn.hidden = unmappedFa.length === 0;
}

function renderIssues() {
  const issues = [];
  const faMissing = state.records.filter((record) => record.fp === "FA" && !record.tinyClienteId);
  const avPending = state.records.filter((record) => record.fp === "AV" && record.avPagamento === "pendente");
  if (faMissing.length) issues.push(`${faMissing.length} cliente(s) FA sem vinculo Tiny.`);
  if (avPending.length) issues.push(`${avPending.length} item(ns) AV sem forma real de pagamento.`);
  if (!state.records.length) issues.push("Nenhuma planilha carregada.");
  if (!issues.length) issues.push("Lote pronto para conferencia final.");
  els.issuesList.innerHTML = issues.map((issue) => `<li>${issue}</li>`).join("");
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
  for (const file of files) {
    try {
      const text = await readFileText(file);
      const records = parseExportedHtml(text, file.name);
      if (!records.length) throw new Error("Nenhum registro valido encontrado.");
      imported.push(...records);
      if (!state.sourceFiles.includes(file.name)) state.sourceFiles.push(file.name);
    } catch (error) {
      errors.push(`${file.name}: ${error.message}`);
    }
  }
  state.records = [...state.records, ...imported];
  render();
  if (errors.length) {
    alert(errors.join("\n"));
  }
});

els.loadSampleBtn.addEventListener("click", loadSample);
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
    alert(`Erro de conexao: ${err.message}`);
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
  const BATCH_SIZE = 15; // evita timeout do Railway (~30s por requisicao)
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
      if (tokenError) {
        msg += `\n\n⚠️ Erro de autenticacao detectado.\nVocê pode reautorizar em: ${window.location.origin}${apiBase}/auth`;
      }
    }
    alert(msg);

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
      <td><strong>${p.cliente}</strong></td>
      <td><span class="status ${p.fp === "FA" ? "ok" : "pending"}">${p.fp}</span></td>
      <td>${p.avPagamento || "—"}</td>
      <td class="amount">${formatMoney(p.valor)}</td>
      <td>${formatDateBr(p.dataVencimento)}</td>
      <td>${p.formaRecebimento}</td>
      <td style="font-size:12px;color:var(--muted)">${p.numeroDocumento}</td>
      <td style="font-size:11px;color:var(--muted)">${p.servico || "—"}</td>
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
      .filter((r) => r.fp === "FA" && !r.tinyClienteId)
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
  const total = state.records.filter((r) => r.fp === "FA" && !r.tinyClienteId).length;
  document.querySelector("#mapProgress").textContent =
    `${mapState.queue.length + 1} de ${total + mapState.queue.length} cliente(s) nao mapeado(s)`;
  document.querySelector("#mapClienteAtual").innerHTML =
    `${mapState.current}<span>Nome como aparece na planilha</span>`;
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
    container.innerHTML = `<p class="map-empty">Erro ao buscar: ${err.message}${authLink}</p>`;
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
          <strong>${c.nome}</strong>
          ${c.fantasia ? `<span>${c.fantasia}</span>` : ""}
        </div>
        <span class="candidate-score ${high ? "high" : ""}">${pct}%</span>
        <button class="candidate-confirm" type="button"
          data-id="${c.id}" data-nome="${c.nome}">Confirmar</button>
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
    alert(`Erro no auto-mapeamento: ${err.message}`);
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
    }
  })
  .catch(() => {});

loadSample();
