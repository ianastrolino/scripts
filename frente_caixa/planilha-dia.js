/**
 * Conferência Antecipada — painel "Planilha do dia" no Caixa.
 * Permite operadora carregar planilha do Sispevi várias vezes ao dia
 * e ver status em tempo real do cruzamento com o PDV.
 *
 * Endpoints:
 * - POST /u/<unit>/api/planilha/upload — persiste planilha do dia
 * - GET  /u/<unit>/api/planilha/status — stats + cruzamento PDV
 *
 * Isolado de caixa.js/caixa2.js — zero risco no caminho crítico.
 */
(function () {
  "use strict";

  const _pathMatch = window.location.pathname.match(/^(\/u\/[^/]+)/);
  const apiBase = _pathMatch ? _pathMatch[1] : "";
  if (!apiBase) return; // só roda em /u/<unidade>/

  // ── Helpers ──────────────────────────────────────────────────────────
  function _escHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function _todayIso() {
    return new Date().toISOString().slice(0, 10);
  }
  async function _csrf() {
    try {
      const r = await fetch("/api/csrf-token", { cache: "no-store" });
      const d = await r.json();
      return d.token || "";
    } catch { return ""; }
  }
  function _readFileText(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = reject;
      reader.readAsText(file);
    });
  }

  // ── Parser de planilha Sispevi (HTML/XLS antigo) ──────────────────────
  // Cópia local do parseExportedHtml de app.js — duplicação consciente
  // pra não acoplar caixa2 ao bundle do fechamento.
  function _normalizeKey(s) {
    return String(s || "").toUpperCase().replace(/[ÁÀÂÃÄ]/g,"A").replace(/[ÉÈÊË]/g,"E")
      .replace(/[ÍÌÎÏ]/g,"I").replace(/[ÓÒÔÕÖ]/g,"O").replace(/[ÚÙÛÜ]/g,"U")
      .replace(/Ç/g,"C").replace(/[^A-Z0-9]/g,"");
  }
  function _cleanText(s) {
    return String(s == null ? "" : s).replace(/\s+/g, " ").trim();
  }
  function _parsePreco(s) {
    if (s == null) return 0;
    const t = String(s).replace(/[R$\s]/g, "").replace(/\./g, "").replace(",", ".");
    const v = parseFloat(t);
    return isNaN(v) ? 0 : v;
  }
  function _parseDataBr(s) {
    const m = String(s || "").match(/(\d{2})\/(\d{2})\/(\d{4})/);
    if (m) return `${m[3]}-${m[2]}-${m[1]}`;
    const iso = String(s || "").match(/(\d{4})-(\d{2})-(\d{2})/);
    if (iso) return iso[0];
    return "";
  }
  function _parseSispeviHtml(text, sourceFile) {
    const doc = new DOMParser().parseFromString(text, "text/html");
    const rows = [...doc.querySelectorAll("tr")].map((tr) =>
      [...tr.children].map((cell) => _cleanText(cell.textContent))
    );
    const headerIdx = rows.findIndex((row) => {
      const keys = row.map(_normalizeKey);
      return ["DATA", "PLACA", "CLIENTE", "SERVICO"].every((k) => keys.includes(k));
    });
    if (headerIdx < 0) return [];
    const header = rows[headerIdx].map(_normalizeKey);
    const idxOf = (k) => header.indexOf(k);
    const out = [];
    for (let i = headerIdx + 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || !row.length) continue;
      const placa = row[idxOf("PLACA")];
      if (!placa) continue;
      const fpRaw = (row[idxOf("FP")] || "").toUpperCase();
      const fp = fpRaw.includes("FA") ? "FA" : fpRaw.includes("DET") ? "detran" : "AV";
      out.push({
        id:        `pln-${i}-${placa}`,
        data:      _parseDataBr(row[idxOf("DATA")]),
        placa:     placa,
        cliente:   row[idxOf("CLIENTE")] || "",
        servico:   row[idxOf("SERVICO")] || "",
        fp:        fp,
        preco:     _parsePreco(row[idxOf("PRECO")] || row[idxOf("VALOR")] || "0"),
        origemArquivo: sourceFile || "",
      });
    }
    return out;
  }

  // ── Estado e UI ──────────────────────────────────────────────────────
  const els = {
    panel:    document.getElementById("planilhaDiaPanel"),
    meta:     document.getElementById("pdMeta"),
    stats:    document.getElementById("pdStats"),
    empty:    document.getElementById("pdEmpty"),
    btnUpload: document.getElementById("pdUploadBtn"),
    btnRefresh: document.getElementById("pdRefreshBtn"),
    fileInput: document.getElementById("pdFileInput"),
  };
  if (!els.panel) return; // pagina nao tem o painel

  let lastStatus = null;

  function _statChip(label, value, cls = "") {
    return `<span class="pd-stat ${cls}"><span>${_escHtml(label)}</span> <strong>${value}</strong></span>`;
  }

  function renderStatus(status) {
    lastStatus = status;
    if (!status || !status.exists) {
      els.meta.textContent = "Nenhuma planilha carregada";
      els.stats.innerHTML = "";
      els.empty.hidden = false;
      return;
    }
    const s = status.stats || {};
    const parts = [
      _statChip("Total", s.total || 0),
      _statChip("Cruzadas", s.cruzadas || 0, "pd-stat-ok"),
    ];
    if (s.divergencias)   parts.push(_statChip("Divergências", s.divergencias, "pd-stat-warn"));
    if (s.orfas_planilha) parts.push(_statChip("Sem PDV", s.orfas_planilha, "pd-stat-bad"));
    if (s.orfas_pdv)      parts.push(_statChip("Órfãs PDV", s.orfas_pdv, "pd-stat-info"));
    if (s.dia_anterior)   parts.push(_statChip("Dia anterior", s.dia_anterior, "pd-stat-info"));
    els.stats.innerHTML = parts.join("");
    const upTime = status.uploaded_at ? status.uploaded_at.slice(11, 16) : "—";
    const versao = status.versao || 1;
    els.meta.textContent = `v${versao} · atualizada ${upTime}`;
    els.empty.hidden = true;
  }

  async function fetchStatus() {
    try {
      const data = _todayIso();
      const r = await fetch(`${apiBase}/api/planilha/status?data=${data}`, { cache: "no-store" });
      const j = await r.json();
      if (j.success) renderStatus(j);
    } catch (e) {
      console.warn("[planilha-dia] status fetch falhou", e);
    }
  }

  async function uploadPlanilha(file) {
    try {
      const text = await _readFileText(file);
      const records = _parseSispeviHtml(text, file.name);
      if (!records.length) {
        alert("Nenhuma vistoria encontrada na planilha. Verifique se é o arquivo correto do Sispevi.");
        return;
      }
      // Data alvo: usa data dos próprios records (planilha pode vir com vistorias de dias diferentes;
      // pra simplicidade usamos a primeira data válida ou hoje)
      let dataIso = _todayIso();
      for (const r of records) { if (r.data) { dataIso = r.data; break; } }
      // Mas se a planilha for do dia anterior inteira, ainda salvamos como "hoje" pra
      // operadora ver no painel — vistorias de outro dia ganham flag dia_anterior
      // no GET /status. Decisão: salvar com data de HOJE sempre.
      dataIso = _todayIso();

      const csrf = await _csrf();
      const r = await fetch(`${apiBase}/api/planilha/upload`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf },
        body: JSON.stringify({ data: dataIso, arquivo: file.name, records }),
      });
      const j = await r.json();
      if (!j.success) {
        alert("Falha no upload: " + (j.error || "desconhecida"));
        return;
      }
      // Aviso se placas sumiram desde o upload anterior (decisão 3 — diff inteligente)
      if (Array.isArray(j.placas_removidas) && j.placas_removidas.length > 0) {
        alert(`Aviso: ${j.placas_removidas.length} placa(s) sumiu/sumiram da planilha:\n\n` +
              j.placas_removidas.slice(0, 10).join(", ") +
              (j.placas_removidas.length > 10 ? `\n…e mais ${j.placas_removidas.length - 10}` : ""));
      }
      await fetchStatus();
    } catch (e) {
      alert("Erro ao processar planilha: " + e.message);
    }
  }

  // ── Wire ──────────────────────────────────────────────────────────────
  els.btnUpload.addEventListener("click", () => els.fileInput.click());
  els.fileInput.addEventListener("change", (e) => {
    const file = e.target.files[0];
    if (file) uploadPlanilha(file);
    e.target.value = ""; // permite re-upload do mesmo arquivo
  });
  els.btnRefresh.addEventListener("click", fetchStatus);

  // Carga inicial
  fetchStatus();
})();
