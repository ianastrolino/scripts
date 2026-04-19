/* caixa2.js — Features exclusivas do Layout 2: Astro chatbot + Resumo do dia */

// ── Estado do chat ────────────────────────────────────────────────────────────
const chatState = {
  open: false,
  messages: [],          // histórico para a API { role, content }
  unread: 0,
  loading: false,
};

const QUICK_REPLIES = [
  "Como faço um lançamento?",
  "O que é Faturado?",
  "Como editar ou excluir?",
  "Como funciona o Fechamento?",
  "Como ver o resumo do dia?",
];

// ── Resumo do dia ─────────────────────────────────────────────────────────────

function calcularResumo() {
  const lcs = state.lancamentos;

  // Por forma de pagamento
  const porFp = { dinheiro: 0, debito: 0, credito: 0, pix: 0, faturado: 0 };
  lcs.forEach(l => { if (l.fp in porFp) porFp[l.fp] += l.valor; });

  // Por serviço
  const porServico = {};
  lcs.forEach(l => {
    if (!porServico[l.servico]) porServico[l.servico] = { count: 0, total: 0 };
    porServico[l.servico].count++;
    porServico[l.servico].total += l.valor;
  });

  // Por cliente
  const porCliente = {};
  lcs.forEach(l => {
    const k = l.cliente.trim();
    if (!porCliente[k]) porCliente[k] = { count: 0, total: 0 };
    porCliente[k].count++;
    porCliente[k].total += l.valor;
  });

  // Por hora
  const porHora = {};
  lcs.forEach(l => {
    const h = (l.hora || "00:00").split(":")[0] + "h";
    porHora[h] = (porHora[h] || 0) + 1;
  });
  const horaPico = Object.entries(porHora).sort((a, b) => b[1] - a[1])[0];

  const totalAvista = porFp.dinheiro + porFp.debito + porFp.credito + porFp.pix;
  const totalGeral  = totalAvista + porFp.faturado;

  return {
    totalGeral, totalAvista, porFp,
    porServico: Object.entries(porServico).sort((a, b) => b[1].total - a[1].total),
    porCliente: Object.entries(porCliente).sort((a, b) => b[1].total - a[1].total).slice(0, 8),
    horaPico: horaPico ? `${horaPico[0]} (${horaPico[1]} atend.)` : "—",
    total: lcs.length,
  };
}

function abrirResumo() {
  if (state.lancamentos.length === 0) {
    alert("Nenhum lancamento registrado hoje.");
    return;
  }
  const r   = calcularResumo();
  const fmt = v => "R$ " + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const hoje = new Date().toLocaleDateString("pt-BR", { weekday: "long", day: "2-digit", month: "long", year: "numeric" });

  const fpRows = [
    ["💵 Dinheiro", r.porFp.dinheiro],
    ["💳 Debito",   r.porFp.debito],
    ["💳 Credito",  r.porFp.credito],
    ["⚡ PIX",      r.porFp.pix],
    ["🧾 Faturado", r.porFp.faturado],
  ].filter(([, v]) => v > 0)
   .map(([label, v]) => `<tr><td>${label}</td><td style="text-align:right;font-weight:700;">${fmt(v)}</td></tr>`)
   .join("");

  const servicoRows = r.porServico
    .map(([s, d]) => `<tr>
      <td>${s}</td>
      <td style="text-align:center;color:var(--muted);">${d.count}x</td>
      <td style="text-align:right;font-weight:700;">${fmt(d.total)}</td>
    </tr>`).join("");

  const clienteRows = r.porCliente
    .map(([c, d]) => `<tr>
      <td>${c}</td>
      <td style="text-align:center;color:var(--muted);">${d.count}x</td>
      <td style="text-align:right;font-weight:700;">${fmt(d.total)}</td>
    </tr>`).join("");

  document.getElementById("resumoContent").innerHTML = `
    <div style="text-align:center;margin-bottom:24px;">
      <div style="font-size:13px;color:var(--muted);text-transform:capitalize;margin-bottom:4px;">${hoje}</div>
      <div style="font-size:36px;font-weight:800;letter-spacing:-.02em;">${fmt(r.totalGeral)}</div>
      <div style="font-size:13px;color:var(--muted);margin-top:4px;">${r.total} lancamento${r.total !== 1 ? "s" : ""} · A vista: ${fmt(r.totalAvista)} · Pico: ${r.horaPico}</div>
    </div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px;">
      <div>
        <div style="font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;">Por pagamento</div>
        <table style="width:100%;font-size:14px;border-collapse:collapse;">${fpRows}</table>
      </div>
      <div>
        <div style="font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;">Por servico</div>
        <table style="width:100%;font-size:13px;border-collapse:collapse;">${servicoRows}</table>
      </div>
    </div>

    ${r.porCliente.length > 0 ? `
    <div>
      <div style="font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;">Por cliente</div>
      <table style="width:100%;font-size:13px;border-collapse:collapse;">${clienteRows}</table>
    </div>` : ""}
  `;

  // Armazena para copiar
  document.getElementById("resumoModal")._resumo = { r, fmt, hoje };
  document.getElementById("resumoModal").classList.add("open");
}

function copiarResumo() {
  const modal = document.getElementById("resumoModal");
  if (!modal._resumo) return;
  const { r, fmt, hoje } = modal._resumo;
  const unidade = document.getElementById("unidadeLabel").textContent || "";

  const linhas = [
    `📊 RESUMO DO CAIXA — ${new Date().toLocaleDateString("pt-BR")}`,
    `🏢 ${unidade}`,
    "",
    `💰 TOTAL GERAL: ${fmt(r.totalGeral)}`,
    `   A vista: ${fmt(r.totalAvista)}`,
    `   Faturado: ${fmt(r.porFp.faturado)}`,
    "",
    "💳 POR PAGAMENTO:",
    r.porFp.dinheiro > 0 ? `   💵 Dinheiro: ${fmt(r.porFp.dinheiro)}` : null,
    r.porFp.debito   > 0 ? `   💳 Debito:   ${fmt(r.porFp.debito)}`   : null,
    r.porFp.credito  > 0 ? `   💳 Credito:  ${fmt(r.porFp.credito)}`  : null,
    r.porFp.pix      > 0 ? `   ⚡ PIX:      ${fmt(r.porFp.pix)}`      : null,
    r.porFp.faturado > 0 ? `   🧾 Faturado: ${fmt(r.porFp.faturado)}` : null,
    "",
    "📋 POR SERVICO:",
    ...r.porServico.map(([s, d]) => `   • ${s} (${d.count}x): ${fmt(d.total)}`),
    "",
    r.porCliente.length > 0 ? "👥 CLIENTES:" : null,
    ...r.porCliente.slice(0, 5).map(([c, d]) => `   • ${c} (${d.count}x): ${fmt(d.total)}`),
    "",
    `Total de lancamentos: ${r.total}`,
    `Horario de pico: ${r.horaPico}`,
  ].filter(l => l !== null).join("\n");

  navigator.clipboard.writeText(linhas).then(() => {
    const btn = document.getElementById("btnCopiarResumo");
    btn.textContent = "✅ Copiado!";
    setTimeout(() => { btn.textContent = "📋 Copiar para WhatsApp"; }, 2500);
  }).catch(() => {
    alert(linhas);
  });
}

// ── Chatbot Astro ─────────────────────────────────────────────────────────────

function toggleChat() {
  chatState.open = !chatState.open;
  const panel = document.getElementById("chatPanel");
  const badge = document.getElementById("chatBadge");
  panel.classList.toggle("open", chatState.open);
  if (chatState.open) {
    chatState.unread = 0;
    badge.style.display = "none";
    setTimeout(() => document.getElementById("chatInput").focus(), 200);
  }
}

function renderMessages() {
  const container = document.getElementById("chatMessages");
  container.innerHTML = chatState.messages.map(m => `
    <div class="chat-msg chat-msg-${m.role}">
      ${m.role === "assistant" ? '<div class="chat-avatar">A</div>' : ""}
      <div class="chat-bubble">${m.content.replace(/\n/g, "<br>")}</div>
    </div>
  `).join("");

  if (chatState.loading) {
    container.innerHTML += `
      <div class="chat-msg chat-msg-assistant">
        <div class="chat-avatar">A</div>
        <div class="chat-bubble chat-typing">
          <span></span><span></span><span></span>
        </div>
      </div>`;
  }
  container.scrollTop = container.scrollHeight;
}

function showQuickReplies() {
  const el = document.getElementById("chatQuickReplies");
  if (chatState.messages.length > 0) { el.style.display = "none"; return; }
  el.innerHTML = QUICK_REPLIES
    .map(q => `<button class="quick-reply-btn" onclick="sendQuick('${q.replace(/'/g, "\\'")}')">${q}</button>`)
    .join("");
  el.style.display = "flex";
}

function sendQuick(text) {
  document.getElementById("chatInput").value = text;
  sendMessage();
}

async function sendMessage() {
  const input = document.getElementById("chatInput");
  const text  = input.value.trim();
  if (!text || chatState.loading) return;

  input.value = "";
  chatState.messages.push({ role: "user", content: text });
  chatState.loading = true;
  renderMessages();
  showQuickReplies();

  try {
    const res = await apiFetch(`${apiBase}/api/astro`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ messages: chatState.messages }),
    });

    chatState.loading = false;

    if (res.success) {
      chatState.messages.push({ role: "assistant", content: res.reply });
    } else {
      chatState.messages.push({ role: "assistant", content: `Erro: ${res.error}` });
    }
  } catch (e) {
    chatState.loading = false;
    if (e.message !== "session_expired") {
      chatState.messages.push({ role: "assistant", content: "Problema de conexao. Tente novamente." });
    }
  }

  if (!chatState.open) {
    chatState.unread++;
    const badge = document.getElementById("chatBadge");
    badge.textContent = chatState.unread;
    badge.style.display = "flex";
  }

  renderMessages();
  showQuickReplies();
}

// ── Pré-Conferência ───────────────────────────────────────────────────────────

const fmtConf = v => "R$ " + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function parseConfVal(elId) {
  const txt = document.getElementById(elId).textContent;
  return parseFloat(txt.replace("R$", "").replace(/\./g, "").replace(",", ".").trim()) || 0;
}

function atualizarBtnConferir() {
  const btn = document.getElementById("btnAbrirConferencia");
  if (!btn) return;
  btn.classList.toggle("visible", state.lancamentos.length > 0);
}

function abrirConferencia() {
  const r = calcularResumo();
  const pdvDin = r.porFp.dinheiro;
  const pdvDeb = r.porFp.debito;
  const pdvCrd = r.porFp.credito;
  const pdvPix = r.porFp.pix;

  document.getElementById("cPdvDin").textContent   = fmtConf(pdvDin);
  document.getElementById("cPdvDeb").textContent   = fmtConf(pdvDeb);
  document.getElementById("cPdvCrd").textContent   = fmtConf(pdvCrd);
  document.getElementById("cPdvPix").textContent   = fmtConf(pdvPix);
  document.getElementById("cPdvTotal").textContent = fmtConf(pdvDin + pdvDeb + pdvCrd + pdvPix);

  ["cFisDin","cFisDeb","cFisCrd","cFisPix"].forEach(id => { document.getElementById(id).value = ""; });
  ["cDiffDin","cDiffDeb","cDiffCrd","cDiffPix","cDiffTotal"].forEach(id => {
    const el = document.getElementById(id);
    el.textContent = "—";
    el.className = "conf-diff";
  });
  const alert = document.getElementById("confAlert");
  alert.className = "conf-alert";
  alert.textContent = "";

  document.getElementById("formCard").style.display = "none";
  document.getElementById("confCard").style.display = "";
  document.getElementById("cFisDin").focus();
}

function voltarLancamentos() {
  document.getElementById("confCard").style.display = "none";
  document.getElementById("formCard").style.display = "";
}

function calcularConferencia() {
  const pdvDin   = parseConfVal("cPdvDin");
  const pdvDeb   = parseConfVal("cPdvDeb");
  const pdvCrd   = parseConfVal("cPdvCrd");
  const pdvPix   = parseConfVal("cPdvPix");
  const totalPdv = pdvDin + pdvDeb + pdvCrd + pdvPix;

  const fisDin = parseFloat(document.getElementById("cFisDin").value) || 0;
  const fisDeb = parseFloat(document.getElementById("cFisDeb").value) || 0;
  const fisCrd = parseFloat(document.getElementById("cFisCrd").value) || 0;
  const fisPix = parseFloat(document.getElementById("cFisPix").value) || 0;
  const totalFis = fisDin + fisDeb + fisCrd + fisPix;

  const inputs = ["cFisDin","cFisDeb","cFisCrd","cFisPix"];
  const anyFilled = inputs.some(id => document.getElementById(id).value !== "");

  const renderDiff = (elId, pdv, fis, filled) => {
    const el = document.getElementById(elId);
    if (!filled) { el.textContent = "—"; el.className = "conf-diff"; return; }
    const diff = fis - pdv;
    const abs  = Math.abs(diff);
    el.textContent = (diff >= 0 ? "+" : "−") + fmtConf(abs);
    el.className   = "conf-diff " + (abs < 0.01 ? "ok" : abs <= 5 ? "warn" : "err");
  };

  renderDiff("cDiffDin",   pdvDin,   fisDin,   document.getElementById("cFisDin").value !== "");
  renderDiff("cDiffDeb",   pdvDeb,   fisDeb,   document.getElementById("cFisDeb").value !== "");
  renderDiff("cDiffCrd",   pdvCrd,   fisCrd,   document.getElementById("cFisCrd").value !== "");
  renderDiff("cDiffPix",   pdvPix,   fisPix,   document.getElementById("cFisPix").value !== "");
  renderDiff("cDiffTotal", totalPdv, totalFis, anyFilled);

  const alertEl = document.getElementById("confAlert");
  const absTotal = Math.abs(totalFis - totalPdv);
  if (!anyFilled) {
    alertEl.className = "conf-alert";
    alertEl.textContent = "";
  } else if (absTotal < 0.01) {
    alertEl.className = "conf-alert ok show";
    alertEl.textContent = "✅ Caixa conferido! Os valores físicos conferem com o PDV.";
  } else if (absTotal <= 10) {
    alertEl.className = "conf-alert warn show";
    alertEl.textContent = `⚠️ Diferença de ${fmtConf(absTotal)}. Verifique antes de prosseguir.`;
  } else {
    alertEl.className = "conf-alert err show";
    alertEl.textContent = `❌ Divergência de ${fmtConf(absTotal)}. Revise os valores antes de fechar o caixa.`;
  }

  return { pdvDin, pdvDeb, pdvCrd, pdvPix, totalPdv, fisDin, fisDeb, fisCrd, fisPix, totalFis };
}

function confirmarConferencia() {
  const c = calcularConferencia();
  sessionStorage.setItem("conferencia_pdv", JSON.stringify({
    pdv:    { dinheiro: c.pdvDin, debito: c.pdvDeb, credito: c.pdvCrd, pix: c.pdvPix, total: c.totalPdv },
    fisico: { dinheiro: c.fisDin, debito: c.fisDeb, credito: c.fisCrd, pix: c.fisPix, total: c.totalFis },
    diff: c.totalFis - c.totalPdv,
    lockedAt: new Date().toISOString(),
  }));
  window.location.href = `${apiBase}/fechamento`;
}

// ── Status bar ────────────────────────────────────────────────────────────────

function atualizarStatusBar() {
  const el = document.getElementById("statusDate");
  if (!el) return;
  const hoje = new Date().toLocaleDateString("pt-BR", {
    weekday: "long", day: "2-digit", month: "long", year: "numeric"
  });
  el.textContent = hoje.charAt(0).toUpperCase() + hoje.slice(1);
}

// ── Init layout 2 ─────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  atualizarStatusBar();

  // Avatar do usuário (inicial do nome)
  const avatarEl = document.getElementById("userAvatar");
  const labelEl  = document.getElementById("userLabel");
  if (avatarEl && labelEl) {
    fetch(`${apiBase}/api/info`).then(r => r.json()).then(info => {
      if (info.unidade) {
        document.getElementById("unidadeLabel").textContent = info.unidade;
        document.getElementById("statusUnit").textContent   = info.unidade;
      }
      if (info.usuario) {
        labelEl.textContent   = info.usuario;
        avatarEl.textContent  = info.usuario.charAt(0).toUpperCase();
      }
      if (apiBase) {
        const linkFechamento = document.getElementById("linkFechamento");
        if (linkFechamento) linkFechamento.href = `${apiBase}/fechamento`;
        const linkCaixaL2 = document.getElementById("linkCaixaL2");
        if (linkCaixaL2) linkCaixaL2.href = `${apiBase}/caixa2`;
        const linkManual = document.getElementById("linkManual");
        if (linkManual) linkManual.href = `${apiBase}/manual`;
      }
    }).catch(() => {});
  }

  // Conferência
  document.getElementById("btnAbrirConferencia")?.addEventListener("click", abrirConferencia);
  document.getElementById("confBackBtn")?.addEventListener("click", voltarLancamentos);
  document.getElementById("btnConferir")?.addEventListener("click", confirmarConferencia);
  ["cFisDin","cFisDeb","cFisCrd","cFisPix"].forEach(id => {
    document.getElementById(id)?.addEventListener("input", calcularConferencia);
  });

  // Botão resumo
  document.getElementById("btnResumo")?.addEventListener("click", abrirResumo);
  document.getElementById("btnCopiarResumo")?.addEventListener("click", copiarResumo);
  document.getElementById("resumoCloseBtn")?.addEventListener("click", () => {
    document.getElementById("resumoModal").classList.remove("open");
  });
  document.getElementById("resumoModal")?.addEventListener("click", e => {
    if (e.target === e.currentTarget) e.currentTarget.classList.remove("open");
  });

  // Chat
  document.getElementById("chatToggleBtn")?.addEventListener("click", toggleChat);
  document.getElementById("chatCloseBtn")?.addEventListener("click", toggleChat);

  document.getElementById("chatInput")?.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  });
  document.getElementById("chatSendBtn")?.addEventListener("click", sendMessage);

  // Mensagem de boas-vindas
  setTimeout(() => {
    chatState.messages.push({
      role: "assistant",
      content: "Oi! Sou o Astro 👋\nPosso te ajudar com qualquer duvida sobre o sistema. Como posso ajudar?",
    });
    renderMessages();
    showQuickReplies();
    chatState.unread = 1;
    const badge = document.getElementById("chatBadge");
    if (badge) { badge.textContent = "1"; badge.style.display = "flex"; }
  }, 1500);
});
