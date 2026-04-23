/* caixa.js — PDV Caixa do Dia (v2.0) */

// Detecta prefixo /u/<unidade> quando rodando no Railway
const _pathMatch = window.location.pathname.match(/^(\/u\/[^/]+)/);
const apiBase = _pathMatch ? _pathMatch[1] : "";

// Estado local
const state = {
  lancamentos: [],
  totais: { dinheiro: 0, debito: 0, credito: 0, pix: 0, total: 0 },
  fpSelecionado: "",
  editFpSelecionado: "",
  pinCallback: null,          // funcao chamada ao confirmar PIN simples
  servicos: [],
  pinConfigurado: false,
  fechado: false,             // dia atual fechado apos envio Tiny — bloqueia novos lancamentos
  fechamento: null,           // {fechado_em, fechado_por, motivo}
};

// ── Helpers ───────────────────────────────────────────────────────────────────

let _csrfToken = null;
async function getCsrfToken(force = false) {
  if (force || !_csrfToken) {
    try {
      const data = await fetch("/api/csrf-token").then(r => r.json());
      _csrfToken = data.token || "";
    } catch { _csrfToken = ""; }
  }
  return _csrfToken;
}

async function apiFetch(path, options = {}) {
  const method = (options.method || "GET").toUpperCase();
  const doFetch = async (token) => {
    const opts = { ...options };
    if (method !== "GET") {
      opts.headers = { ...options.headers, "X-CSRF-Token": token };
    }
    const response = await fetch(path, opts);
    const text = await response.text();
    return { response, text };
  };

  let token = method !== "GET" ? await getCsrfToken() : "";
  let { response, text } = await doFetch(token);

  if (method !== "GET" && response.status === 403) {
    token = await getCsrfToken(true);
    ({ response, text } = await doFetch(token));
  }

  if (!text) throw new Error(`Resposta vazia (HTTP ${response.status}).`);
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`Resposta invalida (HTTP ${response.status}): ${text.substring(0, 200)}`); }
  if (data.session_expired) {
    alert("Sessao expirada. Voce sera redirecionado para o login.");
    window.location.href = "/login";
    throw new Error("session_expired");
  }
  return data;
}

function brl(valor) {
  return "R$ " + Number(valor).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fpLabel(fp) {
  return { dinheiro: "Dinheiro", debito: "Debito", credito: "Credito", pix: "PIX", faturado: "Faturado" }[fp] || fp;
}

// Icones lucide (mesmo estilo do sidebar/app-shell)
const _ICON_SVG = (paths) => `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round">${paths}</svg>`;
const ICON_EDIT  = _ICON_SVG('<path d="M12 20h9"/><path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4z"/>');
const ICON_TRASH = _ICON_SVG('<path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M10 11v6M14 11v6"/>');

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  // Info da unidade (nome, servicos, pin_configurado)
  try {
    const info = await apiFetch(`${apiBase}/api/info`);
    document.getElementById("unidadeLabel").textContent = info.unidade || "";
    document.getElementById("userLabel").textContent = info.usuario || "";

    state.servicos = info.servicos || [];
    state.pinConfigurado = !!info.pin_configurado;

    // Preenche dropdowns de servico
    const opts = state.servicos.map(s => `<option value="${s}">${s}</option>`).join("");
    document.getElementById("fServico").innerHTML = '<option value="">Selecione...</option>' + opts;
    document.getElementById("eServico").innerHTML = '<option value="">Selecione...</option>' + opts;
  } catch (e) {
    if (e.message !== "session_expired") console.error("Erro ao carregar info:", e);
  }

  // Configura links de navegacao
  document.getElementById("linkFechamento").href = `${apiBase}/`;

  // Carrega lancamentos do dia
  await carregarEstado();

  // Foca na placa
  document.getElementById("fPlaca").focus();
}

// ── Estado do dia ─────────────────────────────────────────────────────────────

async function carregarEstado() {
  try {
    const res = await apiFetch(`${apiBase}/api/caixa/estado`);
    if (res.success) {
      state.lancamentos = res.lancamentos || [];
      state.totais = res.totais || {};
      state.fechado = !!res.fechado;
      state.fechamento = res.fechamento || null;
      setTimeout(() => { renderTabela(); renderTotais(); renderFechamentoTarja(); }, 0);
    }
  } catch (e) {
    if (e.message !== "session_expired") console.error("Erro ao carregar estado:", e);
  }
}

function renderFechamentoTarja() {
  const tarja = document.getElementById("fechamentoTarja");
  if (!tarja) return;
  if (!state.fechado) {
    tarja.style.display = "none";
    document.body.classList.remove("caixa-travado");
    return;
  }
  tarja.style.display = "";
  document.body.classList.add("caixa-travado");
  const fech = state.fechamento || {};
  const hora = fech.fechado_em ? new Date(fech.fechado_em).toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" }) : "—";
  const por = fech.fechado_por ? ` · por ${fech.fechado_por}` : "";
  const titleEl = document.getElementById("fechamentoTarjaTitle");
  const subEl = document.getElementById("fechamentoTarjaSub");
  if (titleEl) titleEl.textContent = "Caixa conferido e enviado ao Tiny";
  if (subEl) subEl.textContent = `Fechamento registrado às ${hora}${por}`;
}

async function reabrirCaixaComPin() {
  const pin = prompt("PIN master para reabrir o caixa do dia:");
  if (!pin) return;
  try {
    const res = await apiFetch(`${apiBase}/api/caixa/reabrir`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin }),
    });
    if (!res.success) {
      alert("Erro: " + (res.error || "Falha ao reabrir."));
      return;
    }
    alert("Caixa reaberto. Lançamentos liberados.");
    await carregarEstado();
  } catch (e) {
    if (e.message !== "session_expired") alert("Erro: " + e.message);
  }
}

// ── Render tabela ─────────────────────────────────────────────────────────────

function renderTabela() {
  try {
    const tbody = document.getElementById("pdvBody");
    const table = document.getElementById("pdvTable");
    const empty = document.getElementById("emptyState");

    if (!tbody || !table || !empty) {
      console.warn("[renderTabela] Elementos da tabela nao encontrados no DOM.");
      return;
    }

    if (state.lancamentos.length === 0) {
      table.style.display = "none";
      empty.style.display = "block";
      empty.innerHTML = `
        <div class="empty-icon-ring">📋</div>
        <p>Caixa pronto para o dia.</p>
        <small>Use o formulário ao lado para registrar o primeiro lançamento.</small>
      `;
      return;
    }

    const filtroEl = document.getElementById("filtroLancamentos");
    const filtro = (filtroEl?.value || "").trim().toLowerCase();
    const lista = filtro
      ? state.lancamentos.filter(lc => {
          const hay = `${lc.placa || ""} ${lc.cliente || ""} ${lc.servico || ""} ${lc.cpf || ""}`.toLowerCase();
          return hay.includes(filtro);
        })
      : state.lancamentos;

    if (lista.length === 0) {
      table.style.display = "none";
      empty.style.display = "block";
      empty.innerHTML = `
        <div class="empty-icon-ring">🔍</div>
        <p>Nenhum lançamento corresponde ao filtro.</p>
        <small>Tente outra placa, cliente ou serviço.</small>
      `;
      return;
    }

    table.style.display = "";
    empty.style.display = "none";

    tbody.innerHTML = lista.map((lc, i) => `
      <tr data-id="${lc.id}">
        <td style="color:var(--muted);font-size:12px;">${i + 1}</td>
        <td style="color:var(--muted);font-size:13px;">${lc.hora}</td>
        <td class="td-placa">${lc.placa}</td>
        <td>${escHtml(lc.cliente)}</td>
        <td style="font-size:13px;">${escHtml(lc.servico)}</td>
        <td class="td-valor">${brl(lc.valor)}</td>
        <td><span class="fp-badge ${lc.fp}">${fpLabel(lc.fp)}</span></td>
        <td>
          <div class="td-actions">
            <button class="btn-icon action-btn" title="Editar" onclick="abrirEditar('${lc.id}')">${ICON_EDIT}</button>
            <button class="btn-icon action-btn danger" title="Excluir" onclick="confirmarExcluir('${lc.id}')">${ICON_TRASH}</button>
          </div>
        </td>
      </tr>
    `).join("");
  } catch (e) {
    console.error("[renderTabela] Erro ao renderizar:", e);
  }
}

function escHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ── Chips de serviço no hero ──────────────────────────────────────────────────

function renderServicos() {
  const el = document.getElementById("servicoChips");
  if (!el) return;
  const lcs = state.lancamentos || [];
  if (!lcs.length) { el.innerHTML = ""; return; }

  const stats = {};
  lcs.forEach(lc => {
    const s = (lc.servico || "").trim();
    if (!s) return;
    if (!stats[s]) stats[s] = { count: 0, total: 0 };
    stats[s].count += 1;
    stats[s].total += Number(lc.valor) || 0;
  });

  const MAX = 5;
  const sorted = Object.entries(stats).sort((a, b) => b[1].total - a[1].total);
  const visible = sorted.slice(0, MAX);
  const extra = sorted.length - MAX;

  el.innerHTML = visible.map(([name, s]) => {
    const label = name.length > 18 ? name.slice(0, 17) + "…" : name;
    return `<span class="svc-chip" title="${escHtml(name)} — ${brl(s.total)} (${s.count})">
      ${escHtml(label)}<span class="svc-chip-value">${brl(s.total)}</span><span class="svc-chip-count">×${s.count}</span>
    </span>`;
  }).join("") + (extra > 0
    ? `<span class="svc-chip svc-chip-more">+${extra} mais</span>`
    : "");
}

// ── Render totais ─────────────────────────────────────────────────────────────

let _renderTotaisActive = false;
function renderTotais(totais, count) {
  if (_renderTotaisActive) { console.warn("renderTotais: chamada recursiva bloqueada"); return; }
  _renderTotaisActive = true;
  try {
    const t = totais || state.totais || { dinheiro: 0, debito: 0, credito: 0, pix: 0, faturado: 0, total: 0 };
    console.log("[renderTotais] t=", JSON.stringify(t), "lancamentos=", state.lancamentos.length);
    const n = count !== undefined ? count : state.lancamentos.length;

    const setVal = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };

    setVal("totDinheiro", brl(t.dinheiro    || 0));
    setVal("totDebito",   brl(t.debito      || 0));
    setVal("totCredito",  brl(t.credito     || 0));
    setVal("totPix",      brl(t.pix         || 0));
    setVal("totFaturado", brl(t.faturado    || 0));
    setVal("totAvista",   brl(t.total_avista !== undefined ? t.total_avista : (t.total || 0)));
    setVal("totCount",    `${n} lancamento${n !== 1 ? "s" : ""}`);

    renderServicos();
    if (typeof atualizarBtnConferir === 'function') atualizarBtnConferir();
  } catch (e) {
    console.error("[renderTotais] Erro fatal:", e);
  } finally {
    _renderTotaisActive = false;
  }
}

// ── Formulário de lançamento ──────────────────────────────────────────────────

function selecionarFp(fp, container, stateKey) {
  state[stateKey] = fp;
  container.querySelectorAll(".fp-btn").forEach(btn => {
    btn.classList.toggle("selected", btn.dataset.fp === fp);
  });
  validarFormulario();
}

const PLACA_RE = /^[A-Z]{3}[0-9]([A-Z][0-9]{2}|[0-9]{3})$/;

function validarPlaca(placa) {
  return PLACA_RE.test(placa.toUpperCase().replace(/[-\s]/g, ""));
}

function mascaraCpfCnpj(v) {
  v = v.replace(/\D/g, "").slice(0, 14);
  if (v.length <= 11) {
    if (v.length > 9) return v.replace(/(\d{3})(\d{3})(\d{3})(\d{1,2})/, "$1.$2.$3-$4");
    if (v.length > 6) return v.replace(/(\d{3})(\d{3})(\d{1,3})/, "$1.$2.$3");
    if (v.length > 3) return v.replace(/(\d{3})(\d{1,3})/, "$1.$2");
    return v;
  }
  if (v.length > 12) return v.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{1,2})/, "$1.$2.$3/$4-$5");
  if (v.length > 8)  return v.replace(/(\d{2})(\d{3})(\d{3})(\d{1,4})/, "$1.$2.$3/$4");
  if (v.length > 5)  return v.replace(/(\d{2})(\d{3})(\d{1,3})/, "$1.$2.$3");
  if (v.length > 2)  return v.replace(/(\d{2})(\d{1,3})/, "$1.$2");
  return v;
}

function validarCPF(cpf) {
  const d = cpf.replace(/\D/g, "");
  if (d.length !== 11 || /^(\d)\1{10}$/.test(d)) return false;
  let s = 0;
  for (let i = 0; i < 9; i++) s += +d[i] * (10 - i);
  let r = (s * 10) % 11; if (r >= 10) r = 0;
  if (r !== +d[9]) return false;
  s = 0;
  for (let i = 0; i < 10; i++) s += +d[i] * (11 - i);
  r = (s * 10) % 11; if (r >= 10) r = 0;
  return r === +d[10];
}

function validarCNPJ(cnpj) {
  const d = cnpj.replace(/\D/g, "");
  if (d.length !== 14 || /^(\d)\1{13}$/.test(d)) return false;
  let s = 0, w = [5,4,3,2,9,8,7,6,5,4,3,2];
  for (let i = 0; i < 12; i++) s += +d[i] * w[i];
  let r = s % 11; r = r < 2 ? 0 : 11 - r;
  if (r !== +d[12]) return false;
  s = 0; w = [6,5,4,3,2,9,8,7,6,5,4,3,2];
  for (let i = 0; i < 13; i++) s += +d[i] * w[i];
  r = s % 11; r = r < 2 ? 0 : 11 - r;
  return r === +d[13];
}

function validarCpfCnpj(val) {
  const d = val.replace(/\D/g, "");
  if (d.length === 11) return validarCPF(val);
  if (d.length === 14) return validarCNPJ(val);
  return false;
}

function validarFormulario() {
  const placa   = document.getElementById("fPlaca").value.trim();
  const cliente = document.getElementById("fCliente").value.trim();
  const servico = document.getElementById("fServico").value;
  const valor   = parseFloat(document.getElementById("fValor").value);
  const cpfEl   = document.getElementById("fCpf");
  const cpfErrEl = document.getElementById("cpfError");

  const placaOk = !placa || validarPlaca(placa);
  const errEl   = document.getElementById("placaError");
  if (errEl) {
    const showErr = placa && !placaOk && placa.length >= 7;
    errEl.textContent = showErr ? "Placa inválida. Use AAA0000 ou AAA0A00." : "";
    errEl.style.display = showErr ? "block" : "none";
    document.getElementById("fPlaca").style.borderColor = showErr ? "var(--red)" : "";
  }

  let cpfOk = true;
  const cpfWrap = document.getElementById("fCpfWrap");
  const cpfVisible = cpfWrap && cpfWrap.style.display !== "none";
  if (cpfEl && cpfVisible) {
    const cpfVal = cpfEl.value.trim();
    if (!cpfVal) {
      cpfOk = false;
      if (cpfErrEl) { cpfErrEl.textContent = ""; cpfErrEl.style.display = "none"; }
      cpfEl.style.borderColor = "";
    } else if (!validarCpfCnpj(cpfVal)) {
      cpfOk = false;
      if (cpfErrEl) { cpfErrEl.textContent = "CPF ou CNPJ inválido."; cpfErrEl.style.display = "block"; }
      cpfEl.style.borderColor = "var(--red)";
    } else {
      if (cpfErrEl) { cpfErrEl.textContent = ""; cpfErrEl.style.display = "none"; }
      cpfEl.style.borderColor = "";
    }
  }

  const ok = placa && placaOk && cliente && servico && valor > 0 && state.fpSelecionado && cpfOk;
  document.getElementById("btnLancar").disabled = !ok;
}

function limparFormulario() {
  document.getElementById("fPlaca").value   = "";
  document.getElementById("fCliente").value = "";
  document.getElementById("fServico").value = "";
  document.getElementById("fValor").value   = "";
  const cpfEl = document.getElementById("fCpf");
  if (cpfEl) { cpfEl.value = ""; cpfEl.style.borderColor = ""; }
  const cpfErr = document.getElementById("cpfError");
  if (cpfErr) { cpfErr.textContent = ""; cpfErr.style.display = "none"; }
  state.fpSelecionado = "";
  // Suporta layout 1 (.fp-btn) e layout 2 (.fp-card)
  document.querySelectorAll(".fp-btn, .fp-card").forEach(b => b.classList.remove("selected"));
  document.getElementById("btnLancar").disabled = true;
  document.getElementById("formMsg").textContent = "";
  // Limpa erro de placa
  const placaErr = document.getElementById("placaError");
  if (placaErr) { placaErr.textContent = ""; placaErr.style.display = "none"; }
  document.getElementById("fPlaca").style.borderColor = "";
  document.getElementById("fPlaca").focus();
}

function _genUuid() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  // Fallback (browsers antigos)
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    return (c === "x" ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

// ── Placas recentes (autocomplete do formulario) ─────────────────────────────
const PLACAS_KEY = `caixa.placasRecentes${apiBase.replace(/\//g, "_") || ""}`;
const PLACAS_MAX = 100;

function _loadPlacasRecentes() {
  try {
    const raw = localStorage.getItem(PLACAS_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch { return []; }
}
function _savePlacasRecentes(arr) {
  try { localStorage.setItem(PLACAS_KEY, JSON.stringify(arr.slice(0, PLACAS_MAX))); } catch {}
}
function upsertPlacaRecente(placa, cliente, cpf) {
  if (!placa) return;
  const list = _loadPlacasRecentes();
  const idx = list.findIndex(x => x.placa === placa);
  const entry = { placa, cliente: cliente || "", cpf: cpf || "", lastUsed: Date.now() };
  if (idx >= 0) list.splice(idx, 1);
  list.unshift(entry);
  _savePlacasRecentes(list);
  renderPlacasDatalist();
}
function renderPlacasDatalist() {
  const dl = document.getElementById("dlPlacasRecentes");
  if (!dl) return;
  const list = _loadPlacasRecentes();
  dl.innerHTML = list.map(e =>
    `<option value="${e.placa}">${e.cliente ? escHtml(e.cliente) : ""}</option>`
  ).join("");
}
function autoPreencherPorPlaca(placa) {
  if (!placa) return;
  const entry = _loadPlacasRecentes().find(x => x.placa === placa);
  if (!entry) return;
  const clienteEl = document.getElementById("fCliente");
  const cpfEl = document.getElementById("fCpf");
  if (clienteEl && !clienteEl.value.trim() && entry.cliente) clienteEl.value = entry.cliente;
  if (cpfEl && !cpfEl.value.trim() && entry.cpf) cpfEl.value = mascaraCpfCnpj(entry.cpf);
  validarFormulario();
}

async function lancar() {
  if (state.launching) return;
  state.launching = true;
  const btn = document.getElementById("btnLancar");
  const msg = document.getElementById("formMsg");
  btn.disabled = true;
  msg.textContent = "";

  const payload = {
    placa:   document.getElementById("fPlaca").value.trim().toUpperCase(),
    cliente: document.getElementById("fCliente").value.trim().toUpperCase(),
    cpf:     (document.getElementById("fCpf")?.value || "").replace(/\D/g, ""),
    servico: document.getElementById("fServico").value,
    valor:   parseFloat(document.getElementById("fValor").value),
    fp:      state.fpSelecionado,
  };

  // Reusa o mesmo client_uuid enquanto o payload nao muda — se o POST anterior falhou
  // por timeout/rede mas chegou no servidor, o retry nao duplica (server deduplica).
  const fingerprint = [payload.placa, payload.cliente, payload.cpf, payload.servico, payload.valor, payload.fp].join("|");
  if (!state.pendingAttempt || state.pendingAttempt.fingerprint !== fingerprint) {
    state.pendingAttempt = { uuid: _genUuid(), fingerprint };
  }
  payload.client_uuid = state.pendingAttempt.uuid;

  try {
    let res = await apiFetch(`${apiBase}/api/caixa/lancar`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    // Caixa fechado — pede PIN e reenvia com o PIN junto
    if (!res.success && res.reason === "caixa_fechado") {
      const pin = prompt("Caixa do dia fechado (enviado ao Tiny).\nInforme o PIN master para lançar mesmo assim:");
      if (!pin) {
        msg.textContent = "Lançamento cancelado — caixa fechado.";
        btn.disabled = false;
        return;
      }
      res = await apiFetch(`${apiBase}/api/caixa/lancar`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...payload, pin }),
      });
    }

    if (!res.success) {
      msg.textContent = res.error || "Erro ao lancar.";
      btn.disabled = false;
      return;
    }

    // Sucesso (incluindo resposta dedup do server) — libera a tentativa
    state.pendingAttempt = null;

    // Se o server deduplicou, o lancamento ja esta no array local — nao duplica no front
    const jaExiste = state.lancamentos.some(l => l.id === res.lancamento.id);
    if (!jaExiste) state.lancamentos.push(res.lancamento);
    state.totais = res.totais;

    // Memoriza placa/cliente/cpf para autocomplete futuro
    upsertPlacaRecente(res.lancamento.placa, res.lancamento.cliente, res.lancamento.cpf);

    // Feedback visual imediato
    msg.style.color = "var(--accent)";
    msg.textContent = `Lancado: ${payload.placa} — ${brl(payload.valor)} (${fpLabel(payload.fp)})`;
    setTimeout(() => { msg.textContent = ""; msg.style.color = ""; }, 3000);

    limparFormulario();

    // Renders em frame separado — evita stack overflow por cadeia de inicialização
    setTimeout(() => {
      renderTabela();
      renderTotais(res.totais, state.lancamentos.length);
    }, 0);

    // Scroll para a ultima linha
    setTimeout(() => {
      const tbody = document.getElementById("pdvBody");
      if (tbody.lastElementChild) {
        tbody.lastElementChild.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    }, 50);

  } catch (e) {
    if (e.message !== "session_expired") {
      msg.textContent = e.message || "Erro inesperado.";
    }
    btn.disabled = false;
  } finally {
    state.launching = false;
  }
}

// ── Editar lançamento ─────────────────────────────────────────────────────────

function abrirEditar(id) {
  const lc = state.lancamentos.find(l => l.id === id);
  if (!lc) return;

  document.getElementById("editId").value      = lc.id;
  document.getElementById("ePlaca").value       = lc.placa;
  document.getElementById("eCliente").value     = lc.cliente;
  const eCpfEl = document.getElementById("eCpf");
  if (eCpfEl) eCpfEl.value = lc.cpf ? mascaraCpfCnpj(lc.cpf) : "";
  document.getElementById("eServico").value     = lc.servico;
  document.getElementById("eValor").value       = lc.valor;
  document.getElementById("ePinInput").value    = "";
  document.getElementById("editError").textContent = "";

  // Seleciona FP
  state.editFpSelecionado = lc.fp;
  document.querySelectorAll("#editFpButtons .fp-btn").forEach(btn => {
    btn.classList.toggle("selected", btn.dataset.fp === lc.fp);
  });

  document.getElementById("editModal").classList.add("open");
  setTimeout(() => document.getElementById("ePlaca").focus(), 50);
}

function fecharEditar() {
  document.getElementById("editModal").classList.remove("open");
  state.editFpSelecionado = "";
}

async function salvarEdicao() {
  const id      = document.getElementById("editId").value;
  const pin     = document.getElementById("ePinInput").value.trim();
  const errEl   = document.getElementById("editError");
  const btn     = document.getElementById("editConfirmBtn");
  errEl.textContent = "";

  if (!state.pinConfigurado) {
    errEl.textContent = "PIN master nao configurado na unidade.";
    return;
  }
  if (!pin) {
    errEl.textContent = "Digite o PIN para confirmar.";
    document.getElementById("ePinInput").focus();
    return;
  }
  if (!state.editFpSelecionado) {
    errEl.textContent = "Selecione a forma de pagamento.";
    return;
  }

  const payload = {
    pin,
    placa:   document.getElementById("ePlaca").value.trim().toUpperCase(),
    cliente: document.getElementById("eCliente").value.trim(),
    cpf:     (document.getElementById("eCpf")?.value || "").replace(/\D/g, ""),
    servico: document.getElementById("eServico").value,
    valor:   parseFloat(document.getElementById("eValor").value),
    fp:      state.editFpSelecionado,
  };

  btn.disabled = true;
  try {
    const res = await apiFetch(`${apiBase}/api/caixa/editar/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.success) {
      errEl.textContent = res.error || "Erro ao editar.";
      btn.disabled = false;
      return;
    }

    // Atualiza estado local
    const idx = state.lancamentos.findIndex(l => l.id === id);
    if (idx >= 0) {
      state.lancamentos[idx] = { ...state.lancamentos[idx], ...payload };
      delete state.lancamentos[idx].pin;
    }
    state.totais = res.totais;
    renderTabela();
    renderTotais(res.totais, state.lancamentos.length);
    fecharEditar();
  } catch (e) {
    if (e.message !== "session_expired") errEl.textContent = e.message || "Erro inesperado.";
    btn.disabled = false;
  }
}

// ── Excluir lançamento ────────────────────────────────────────────────────────

function confirmarExcluir(id) {
  const lc = state.lancamentos.find(l => l.id === id);
  if (!lc) return;

  document.getElementById("pinModalTitle").textContent = "Excluir lancamento";
  document.getElementById("pinModalDesc").textContent =
    `${lc.placa} — ${escHtml(lc.servico)} — ${brl(lc.valor)} (${fpLabel(lc.fp)})`;
  document.getElementById("pinInput").value = "";
  document.getElementById("pinError").textContent = "";
  document.getElementById("pinModal").classList.add("open");
  setTimeout(() => document.getElementById("pinInput").focus(), 50);

  state.pinCallback = async (pin) => {
    const res = await apiFetch(`${apiBase}/api/caixa/excluir/${id}`, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin }),
    });
    if (!res.success) throw new Error(res.error || "Erro ao excluir.");
    state.lancamentos = state.lancamentos.filter(l => l.id !== id);
    state.totais = res.totais;
    renderTabela();
    renderTotais(res.totais, state.lancamentos.length);
  };
}

function fecharPinModal() {
  document.getElementById("pinModal").classList.remove("open");
  state.pinCallback = null;
}

async function confirmarPin() {
  const pin   = document.getElementById("pinInput").value.trim();
  const errEl = document.getElementById("pinError");
  const btn   = document.getElementById("pinConfirmBtn");
  errEl.textContent = "";

  if (!state.pinConfigurado) {
    errEl.textContent = "PIN master nao configurado na unidade.";
    return;
  }
  if (!pin) {
    errEl.textContent = "Digite o PIN.";
    document.getElementById("pinInput").focus();
    return;
  }

  btn.disabled = true;
  try {
    if (state.pinCallback) await state.pinCallback(pin);
    fecharPinModal();
  } catch (e) {
    errEl.textContent = e.message || "Erro inesperado.";
  }
  btn.disabled = false;
}

// ── Eventos ───────────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  init();

  const btnReabrir = document.getElementById("btnReabrirCaixa");
  if (btnReabrir) btnReabrir.addEventListener("click", reabrirCaixaComPin);

  // Campos de texto: validar ao digitar
  ["fPlaca", "fCliente", "fServico", "fValor", "fCpf"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener("input", validarFormulario);
  });

  // Cliente: força maiúsculo
  document.getElementById("fCliente").addEventListener("input", function () {
    const pos = this.selectionStart;
    this.value = this.value.toUpperCase();
    this.setSelectionRange(pos, pos);
  });

  // CPF/CNPJ: máscara com cursor estável
  function bindCpfMask(el) {
    if (!el) return;
    el.addEventListener("input", function () {
      const digitsBeforeCursor = this.value.slice(0, this.selectionStart).replace(/\D/g, "").length;
      const masked = mascaraCpfCnpj(this.value);
      this.value = masked;
      let digits = 0, pos = 0;
      for (; pos < masked.length; pos++) {
        if (/\d/.test(masked[pos])) digits++;
        if (digits === digitsBeforeCursor) { pos++; break; }
      }
      this.setSelectionRange(pos, pos);
    });
  }
  bindCpfMask(document.getElementById("fCpf"));
  bindCpfMask(document.getElementById("eCpf"));

  // Placa: apenas A-Z 0-9, maiúsculo, máx 7 chars — cobre digitação e paste
  document.getElementById("fPlaca").addEventListener("input", function () {
    const clean = this.value.toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 7);
    if (this.value !== clean) this.value = clean;
  });

  // Autocomplete: ao escolher uma placa do datalist (ou digitar uma ja salva),
  // preenche cliente/cpf se ainda estiverem vazios. Change dispara no select.
  document.getElementById("fPlaca").addEventListener("change", function () {
    autoPreencherPorPlaca(this.value.trim().toUpperCase());
  });
  document.getElementById("fPlaca").addEventListener("blur", function () {
    autoPreencherPorPlaca(this.value.trim().toUpperCase());
  });

  renderPlacasDatalist();

  // Filtro da tabela de lançamentos
  const filtroEl = document.getElementById("filtroLancamentos");
  if (filtroEl) {
    filtroEl.value = "";
    filtroEl.addEventListener("input", () => renderTabela());
  }

  document.getElementById("ePlaca").addEventListener("input", function () {
    const pos = this.selectionStart;
    this.value = this.value.toUpperCase();
    this.setSelectionRange(pos, pos);
  });

  // FP buttons (formulário principal — Layout 1 usa .pdv-form-panel .fp-buttons)
  const fpContainer = document.querySelector(".pdv-form-panel .fp-buttons");
  if (fpContainer) {
    fpContainer.querySelectorAll(".fp-btn").forEach(btn => {
      btn.addEventListener("click", () => selecionarFp(btn.dataset.fp, fpContainer, "fpSelecionado"));
      btn.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          selecionarFp(btn.dataset.fp, fpContainer, "fpSelecionado");
        }
      });
    });
  }

  // FP buttons (modal editar — Layout 1 usa #editFpButtons)
  const editFpContainer = document.getElementById("editFpButtons");
  if (editFpContainer) {
    editFpContainer.querySelectorAll(".fp-btn").forEach(btn => {
      btn.addEventListener("click", () => selecionarFp(btn.dataset.fp, editFpContainer, "editFpSelecionado"));
    });
  }

  // Botao lancar
  document.getElementById("btnLancar").addEventListener("click", lancar);

  // Enter no formulario avanca campos / lanca
  document.getElementById("fPlaca").addEventListener("keydown", e => {
    if (e.key === "Enter") { e.preventDefault(); document.getElementById("fCliente").focus(); }
  });
  document.getElementById("fCliente").addEventListener("keydown", e => {
    if (e.key === "Enter") {
      e.preventDefault();
      const cpfWrap = document.getElementById("fCpfWrap");
      if (cpfWrap && cpfWrap.style.display !== "none") document.getElementById("fCpf").focus();
      else document.getElementById("fServico").focus();
    }
  });
  const cpfInputNav = document.getElementById("fCpf");
  if (cpfInputNav) {
    cpfInputNav.addEventListener("keydown", e => {
      if (e.key === "Enter") { e.preventDefault(); document.getElementById("fServico").focus(); }
    });
  }
  document.getElementById("fServico").addEventListener("keydown", e => {
    if (e.key === "Enter") { e.preventDefault(); document.getElementById("fValor").focus(); }
  });
  document.getElementById("fValor").addEventListener("keydown", e => {
    if (e.key === "Enter") {
      e.preventDefault();
      // Se FP nao selecionado, foca no primeiro botao FP
      if (!state.fpSelecionado) {
        if (fpContainer) fpContainer.querySelector(".fp-btn").focus();
      } else if (!document.getElementById("btnLancar").disabled) {
        document.getElementById("btnLancar").click();
      }
    }
  });

  // Modal PIN
  document.getElementById("pinCancelBtn").addEventListener("click", fecharPinModal);
  document.getElementById("pinConfirmBtn").addEventListener("click", confirmarPin);
  document.getElementById("pinInput").addEventListener("keydown", e => {
    if (e.key === "Enter") confirmarPin();
    if (e.key === "Escape") fecharPinModal();
  });
  document.getElementById("pinModal").addEventListener("click", e => {
    if (e.target === e.currentTarget) fecharPinModal();
  });

  // Modal Editar
  document.getElementById("editCancelBtn").addEventListener("click", fecharEditar);
  document.getElementById("editConfirmBtn").addEventListener("click", salvarEdicao);
  document.getElementById("ePinInput").addEventListener("keydown", e => {
    if (e.key === "Enter") salvarEdicao();
    if (e.key === "Escape") fecharEditar();
  });
  document.getElementById("editModal").addEventListener("click", e => {
    if (e.target === e.currentTarget) fecharEditar();
  });

  // Esc fecha modais
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") {
      fecharPinModal();
      fecharEditar();
    }
  });

  // Hotkeys 1-6 para forma de pagamento — so dispara fora de input/select/textarea
  // e com os modais fechados (evita mudar FP por tras da tela de confirmacao)
  document.addEventListener("keydown", e => {
    if (e.ctrlKey || e.altKey || e.metaKey) return;
    const tag = (e.target.tagName || "").toUpperCase();
    if (tag === "INPUT" || tag === "SELECT" || tag === "TEXTAREA" || e.target.isContentEditable) return;
    if (document.querySelector("#confirmLancarModal.open, #pinModal.open, #editModal.open, #resumoModal.open")) return;
    const formCard = document.getElementById("formCard");
    if (!formCard || formCard.style.display === "none") return;
    const btn = document.querySelector(`#fpGrid .fp-card[data-fp-key="${e.key}"]`);
    if (!btn) return;
    e.preventDefault();
    btn.click();
    btn.focus();
  });
});
