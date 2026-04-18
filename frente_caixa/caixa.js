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
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function apiFetch(path, options = {}) {
  const response = await fetch(path, options);
  const text = await response.text();
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
      setTimeout(() => { renderTabela(); renderTotais(); }, 0);
    }
  } catch (e) {
    if (e.message !== "session_expired") console.error("Erro ao carregar estado:", e);
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
      return;
    }

    table.style.display = "";
    empty.style.display = "none";

    tbody.innerHTML = state.lancamentos.map((lc, i) => `
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
            <button class="btn-icon" title="Editar" onclick="abrirEditar('${lc.id}')">✏️</button>
            <button class="btn-icon" title="Excluir" onclick="confirmarExcluir('${lc.id}')">🗑️</button>
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

  try {
    const res = await apiFetch(`${apiBase}/api/caixa/lancar`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.success) {
      msg.textContent = res.error || "Erro ao lancar.";
      btn.disabled = false;
      return;
    }

    // Atualiza estado local
    state.lancamentos.push(res.lancamento);
    state.totais = res.totais;

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

  // CPF: máscara e validação
  const cpfInput = document.getElementById("fCpf");
  if (cpfInput) {
    cpfInput.addEventListener("input", function () {
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

  // Placa: apenas A-Z 0-9, maiúsculo, máx 7 chars — cobre digitação e paste
  document.getElementById("fPlaca").addEventListener("input", function () {
    const clean = this.value.toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 7);
    if (this.value !== clean) this.value = clean;
  });

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
});
