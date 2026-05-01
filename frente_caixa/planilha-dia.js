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
 * Parser HTML do Sispevi copiado de app.js (parseExportedHtml) — mesma
 * lógica que funciona em produção no Fechamento.
 */
(function () {
  "use strict";

  const _pathMatch = window.location.pathname.match(/^(\/u\/[^/]+)/);
  const apiBase = _pathMatch ? _pathMatch[1] : "";
  if (!apiBase) return;

  // ── Helpers (cópia de app.js) ─────────────────────────────────────────
  function cleanText(value) {
    return String(value || "").replace(/ /g, " ").replace(/\s+/g, " ").trim();
  }
  function removeAccents(value) {
    return cleanText(value).normalize("NFD").replace(/[̀-ͯ]/g, "");
  }
  function normalizeKey(value) {
    return removeAccents(value).toUpperCase().replace(/[^A-Z0-9]+/g, " ").trim();
  }
  function parseMoney(value) {
    const cleaned = cleanText(value).replace(/[^\d,.-]/g, "");
    if (!cleaned) return 0;
    if (cleaned.includes(",")) return Number(cleaned.replace(/\./g, "").replace(",", "."));
    return Number(cleaned);
  }
  function parseDateBr(value) {
    const text = cleanText(value);
    const match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (!match) return "";
    const day = match[1].padStart(2, "0");
    const month = match[2].padStart(2, "0");
    return `${match[3]}-${month}-${day}`;
  }
  function normalizePlate(value) {
    return removeAccents(value).toUpperCase().replace(/[^A-Z0-9]/g, "");
  }
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

  function makeRecord(row, index, sourceFile) {
    const [data, modelo, placa, cliente, servico, fp, preco] = row;
    return {
      id:        `${sourceFile}-${index}`,
      data:      parseDateBr(data),
      modelo:    cleanText(modelo).toUpperCase(),
      placa:     normalizePlate(placa),
      cliente:   cleanText(cliente).toUpperCase(),
      servico:   cleanText(servico).toUpperCase(),
      fp:        cleanText(fp).toUpperCase(),
      preco:     parseMoney(preco),
      origemArquivo: sourceFile,
      linhaOrigem: index + 2,
    };
  }

  // Heuristicas de detecção (Sispevi exporta SEM header — só dados)
  const RE_DATA = /^\d{1,2}[\/\-\. ]\d{1,2}[\/\-\. ]\d{4}$/;
  const RE_PLACA_OLD = /^[A-Z]{3}[\s-]?\d{4}$/;          // antiga: ABC-1234
  const RE_PLACA_MERCOSUL = /^[A-Z]{3}\d[A-Z]\d{2}$/;    // mercosul: ABC1D23
  function isData(s) {
    return RE_DATA.test(cleanText(s).replace(/\s+/g, " "));
  }
  function normalizeDataInput(s) {
    // converte "25 04 2026" / "25.04.2026" / "25-04-2026" pra "25/04/2026"
    const t = cleanText(s).replace(/[\.\-\s]+/g, "/");
    return /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(t) ? t : "";
  }
  function isPlaca(s) {
    const t = cleanText(s).replace(/[\s-]/g, "").toUpperCase();
    return RE_PLACA_OLD.test(t) || RE_PLACA_MERCOSUL.test(t);
  }
  function lastValor(cells, fromIdx) {
    // Procura último valor monetário > 0 nas células a partir de fromIdx
    for (let i = cells.length - 1; i >= fromIdx; i--) {
      const v = parseMoney(cells[i]);
      if (v > 0) return v;
    }
    return 0;
  }
  function detectFp(cells, fromIdx) {
    for (let i = cells.length - 1; i >= fromIdx; i--) {
      const c = cleanText(cells[i]).toUpperCase();
      if (c === "AV" || c === "FA" || c === "DETRAN") return c;
      if (c === "FATURADO" || c === "BOLETO") return "FA";
    }
    return "AV";
  }

  let _debugHeaders = [];

  function parseExportedHtml(text, sourceFile) {
    const doc = new DOMParser().parseFromString(text, "text/html");
    const rows = [...doc.querySelectorAll("tr")].map((tr) =>
      [...tr.children].map((cell) => cleanText(cell.textContent))
    );
    console.log(`[planilha-dia] ${sourceFile}: ${rows.length} rows totais`);

    // Tentativa 1: parser POR HEADER (compatibilidade com planilhas que têm header)
    const REQUIRED_FULL = ["DATA", "MODELO", "PLACA", "CLIENTE", "SERVICO", "FP", "PRECO"];
    let headerIndex = rows.findIndex((row) => {
      const keys = row.map(normalizeKey);
      return REQUIRED_FULL.every((k) => keys.includes(k));
    });
    if (headerIndex >= 0) {
      console.log(`[planilha-dia] ${sourceFile}: header completo encontrado em row ${headerIndex}`);
      const headers = rows[headerIndex].map(normalizeKey);
      const col = {
        data: headers.indexOf("DATA"), modelo: headers.indexOf("MODELO"),
        placa: headers.indexOf("PLACA"), cliente: headers.indexOf("CLIENTE"),
        servico: headers.indexOf("SERVICO"), fp: headers.indexOf("FP"),
        preco: headers.indexOf("PRECO"),
      };
      return rows.slice(headerIndex + 1).reduce((acc, row, i) => {
        const d = row[col.data] || "";
        if (!/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(d)) return acc;
        acc.push(makeRecord([
          d, row[col.modelo], row[col.placa], row[col.cliente],
          row[col.servico], row[col.fp], row[col.preco],
        ], i, sourceFile));
        return acc;
      }, []);
    }

    // Tentativa 2: parser SEM header (Sispevi) — detecta linhas que contêm
    // data válida + placa válida em qualquer posição
    const out = [];
    let idx = 0;
    for (const row of rows) {
      if (row.length < 6) continue;
      let dataIdx = -1, placaIdx = -1;
      for (let i = 0; i < row.length; i++) {
        if (dataIdx < 0 && isData(row[i])) dataIdx = i;
        if (placaIdx < 0 && isPlaca(row[i])) placaIdx = i;
        if (dataIdx >= 0 && placaIdx >= 0) break;
      }
      if (dataIdx < 0 || placaIdx < 0) continue;

      const dataNorm = normalizeDataInput(row[dataIdx]);
      const placaRaw = row[placaIdx];
      // Layout típico Sispevi:
      //   ... | DATA | MODELO | ANO | COR | CHASSI | PLACA | CLIENTE_TIPO | SERVICO | ... | FP | VALOR
      // Modelo fica logo depois da data; sanity check rejeita se for so numero
      // (caso a planilha venha sem coluna modelo, dataIdx+1 cai no ano).
      const modeloRaw = String(row[dataIdx + 1] || "").trim();
      const modelo = /^\d+$/.test(modeloRaw) ? "" : modeloRaw;
      const cliente = row[placaIdx + 1] || "";
      const servico = row[placaIdx + 2] || "";
      const fp      = detectFp(row, placaIdx + 3);
      const valor   = lastValor(row, placaIdx + 3);

      out.push(makeRecord([dataNorm, modelo, placaRaw, cliente, servico, fp, valor], idx++, sourceFile));
    }
    console.log(`[planilha-dia] ${sourceFile}: parser sem-header detectou ${out.length} vistorias`);

    if (out.length === 0) {
      // Coleta amostra das primeiras 5 rows pra debug
      _debugHeaders = rows.slice(0, 5).map((row, i) => `[row ${i}] ${row.slice(0, 12).join(" | ")}`);
      console.warn(`[planilha-dia] ${sourceFile}: nada detectado. Primeiras rows:`, _debugHeaders);
    }
    return out;
  }

  // ── UI ────────────────────────────────────────────────────────────────
  const els = {
    panel:     document.getElementById("planilhaDiaPanel"),
    meta:      document.getElementById("pdMeta"),
    stats:     document.getElementById("pdStats"),
    empty:     document.getElementById("pdEmpty"),
    btnUpload: document.getElementById("pdUploadBtn"),
    btnRefresh: document.getElementById("pdRefreshBtn"),
    fileInput: document.getElementById("pdFileInput"),
    toggleDetails: document.getElementById("pdToggleDetails"),
    toggleLabel:   document.getElementById("pdToggleLabel"),
    details:   document.getElementById("pdDetails"),
  };
  if (!els.panel) return;
  let _detailsOpen = false;
  let _lastStatus = null;

  function _statChip(label, value, cls = "") {
    return `<span class="pd-stat ${cls}"><span>${_escHtml(label)}</span> <strong>${value}</strong></span>`;
  }

  function _fmtBrl(v) {
    return "R$ " + Number(v || 0).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function renderStatus(status) {
    _lastStatus = status;
    if (!status || !status.exists) {
      els.meta.textContent = "Nenhuma planilha carregada";
      els.stats.innerHTML = "";
      els.empty.hidden = false;
      els.toggleDetails.hidden = true;
      els.details.hidden = true;
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
    const arquivos = status.arquivo ? ` · ${status.arquivo}` : "";
    els.meta.textContent = `v${versao} · atualizada ${upTime}${arquivos}`;
    els.empty.hidden = true;

    // Toggle "Mostrar detalhes" se houver algo pra mostrar
    const linhas = status.linhas || [];
    const orfasPdv = status.orfas_pdv || [];
    const semPdv = linhas.filter((l) => l.status === "sem_pdv" && !l.dia_anterior);
    const divergencias = linhas.filter((l) => l.status === "divergencia_valor");
    const diaAnterior = linhas.filter((l) => l.dia_anterior);
    const totalDetalhes = orfasPdv.length + semPdv.length + divergencias.length + diaAnterior.length;
    els.toggleDetails.hidden = totalDetalhes === 0;
    els.toggleLabel.textContent = `${_detailsOpen ? "Ocultar" : "Mostrar"} detalhes (${totalDetalhes})`;
    els.toggleDetails.classList.toggle("is-open", _detailsOpen);

    if (_detailsOpen) renderDetails({ orfasPdv, semPdv, divergencias, diaAnterior });
    else els.details.hidden = true;
  }

  function _cliFmt(c) {
    if (!c) return "";
    const t = String(c).trim();
    if (!t) return "";
    return ` · <span class="pd-cliente">${_escHtml(t)}</span>`;
  }

  function _fillPayload(l) {
    return _escHtml(JSON.stringify({
      placa: l.placa, cliente: l.cliente, servico: l.servico, valor: l.preco, fp: l.fp,
    }));
  }

  function _itemSemPdv(l) {
    const valor = _fmtBrl(l.preco);
    return `<div class="pd-item tipo-sem-pdv">
      <div class="pd-item-desc">
        <span class="pd-mono">${_escHtml(l.placa || "—")}</span>${_cliFmt(l.cliente)} · ${_escHtml(l.servico || "—")} · <strong>${valor}</strong>
        <em>(${_escHtml(l.fp || "AV")})</em>
      </div>
      <button type="button" class="pd-item-acao" data-fill='${_fillPayload(l)}'>+ Lançar agora</button>
    </div>`;
  }

  function _itemDiaAnterior(l) {
    const valor = _fmtBrl(l.preco);
    const fa = (l.fp || "").toUpperCase() === "FA";
    const acao = fa
      ? `<button type="button" class="pd-item-acao ghost" disabled title="FA = boleto, fica em contas a receber">FA — sem ação</button>`
      : `<button type="button" class="pd-item-acao" data-fill='${_fillPayload(l)}'>+ Lançar hoje</button>`;
    return `<div class="pd-item tipo-anterior">
      <div class="pd-item-desc">
        📅 <span class="pd-mono">${_escHtml(l.placa || "—")}</span>${_cliFmt(l.cliente)} · ${_escHtml(l.servico || "—")} · <strong>${valor}</strong>
        <em>(${_escHtml(l.fp || "AV")} · ${_escHtml(l.data || "")})</em>
      </div>
      ${acao}
    </div>`;
  }

  function _itemDivergencia(l) {
    const valor = _fmtBrl(l.preco);
    const pdvVal = l.pdv_match ? _fmtBrl(l.pdv_match.valor) : "—";
    return `<div class="pd-item tipo-div">
      <div class="pd-item-desc">
        <span class="pd-mono">${_escHtml(l.placa || "—")}</span>${_cliFmt(l.cliente)} · ${_escHtml(l.servico || "—")} ·
        planilha <strong>${valor}</strong> ↔ PDV <strong>${pdvVal}</strong>
      </div>
    </div>`;
  }

  function _itemOrfaPdv(o) {
    const valor = _fmtBrl(o.valor);
    return `<div class="pd-item tipo-orfa-pdv">
      <div class="pd-item-desc">
        <span class="pd-mono">${_escHtml(o.placa || "—")}</span>${_cliFmt(o.cliente)} · ${_escHtml(o.servico || "—")} · <strong>${valor}</strong>
        <em>(${_escHtml(o.fp || "AV")} · ${_escHtml(o.hora || "")})</em>
      </div>
    </div>`;
  }

  function renderDetails({ orfasPdv, semPdv, divergencias, diaAnterior }) {
    const blocks = [];
    if (semPdv.length) {
      blocks.push(`<div class="pd-grupo-titulo tipo-sem-pdv">À Vista na planilha sem PDV (${semPdv.length})</div>`);
      blocks.push(...semPdv.map(_itemSemPdv));
    }
    if (orfasPdv.length) {
      blocks.push(`<div class="pd-grupo-titulo tipo-orfa-pdv">Vistorias no PDV sem planilha (${orfasPdv.length})</div>`);
      blocks.push(...orfasPdv.map(_itemOrfaPdv));
    }
    if (divergencias.length) {
      blocks.push(`<div class="pd-grupo-titulo tipo-div">Divergências de valor (${divergencias.length})</div>`);
      blocks.push(...divergencias.map(_itemDivergencia));
    }
    if (diaAnterior.length) {
      blocks.push(`<div class="pd-grupo-titulo tipo-anterior">Vistorias de dia anterior (${diaAnterior.length})</div>`);
      blocks.push(...diaAnterior.map(_itemDiaAnterior));
    }
    els.details.innerHTML = blocks.join("");
    els.details.hidden = false;
    // Wire botões de fill
    els.details.querySelectorAll("[data-fill]").forEach((btn) => {
      btn.addEventListener("click", () => {
        try {
          const data = JSON.parse(btn.dataset.fill);
          fillFormPdv(data);
          // Marca o botão como em-uso pra evitar duplo-clique e dar
          // feedback visual. Item volta ao normal no próximo fetchStatus
          // (auto a cada 2min ou imediato após lançamento via evento)
          btn.disabled = true;
          btn.textContent = "✓ Em preenchimento";
          btn.classList.add("ghost");
          btn.dataset.placaPendente = (data.placa || "").toUpperCase();
        } catch (e) {
          console.warn("[planilha-dia] fill payload inválido", e);
        }
      });
    });
  }

  // Pré-preenche o form #formCard do PDV com os dados da vistoria selecionada
  function fillFormPdv({ placa, cliente, servico, valor, fp }) {
    const fPlaca = document.getElementById("fPlaca");
    const fCliente = document.getElementById("fCliente");
    const fServico = document.getElementById("fServico");
    const fValor = document.getElementById("fValor");
    if (!fPlaca || !fServico || !fValor) {
      alert("Form de novo lançamento não encontrado nesta tela.");
      return;
    }
    fPlaca.value = (placa || "").toUpperCase();
    fPlaca.dispatchEvent(new Event("input", { bubbles: true }));
    if (fCliente && cliente) {
      fCliente.value = String(cliente).toUpperCase();
      fCliente.dispatchEvent(new Event("input", { bubbles: true }));
    }
    fValor.value = Number(valor || 0).toFixed(2);
    fValor.dispatchEvent(new Event("input", { bubbles: true }));

    // Tenta selecionar serviço por matching de texto
    if (servico && fServico.tagName === "SELECT") {
      const target = String(servico).toUpperCase().trim();
      let match = null;
      for (const opt of fServico.options) {
        const t = (opt.text || "").toUpperCase();
        if (t.includes(target) || target.includes(t.trim())) { match = opt; break; }
      }
      if (match) {
        fServico.value = match.value;
        fServico.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }

    // Scroll suave + foco no FP
    document.getElementById("formCard")?.scrollIntoView({ behavior: "smooth", block: "start" });
    setTimeout(() => {
      const fpBtn = document.querySelector(`#fpGrid [data-fp="${fp || "dinheiro"}"]`);
      (fpBtn || fValor).focus();
    }, 350);
  }

  let _fetching = false;
  let _lastErrorAt = 0;

  async function fetchStatus({ silent = false } = {}) {
    if (_fetching) return; // evita corrida (auto-refresh + manual)
    _fetching = true;
    if (!silent) {
      els.btnRefresh?.classList.add("is-loading");
      const prev = els.meta.textContent;
      if (_lastStatus?.exists) {
        els.meta.textContent = prev.replace(/atualizada \S+/, "atualizando…");
      }
    }
    try {
      const data = _todayIso();
      const r = await fetch(`${apiBase}/api/planilha/status?data=${data}`, { cache: "no-store" });
      const j = await r.json();
      if (j.success) {
        _lastErrorAt = 0;
        renderStatus(j);
      } else {
        throw new Error(j.error || "resposta sem success");
      }
    } catch (e) {
      _lastErrorAt = Date.now();
      console.warn("[planilha-dia] status fetch falhou:", e.message || e);
      // Indicador discreto de erro: append no meta
      if (els.meta && _lastStatus?.exists) {
        els.meta.textContent = els.meta.textContent.replace(/ · ⚠.*$/, "") + " · ⚠ erro ao recalcular";
      }
    } finally {
      _fetching = false;
      els.btnRefresh?.classList.remove("is-loading");
    }
  }

  // Multi-upload: parseia N arquivos, combina records, manda 1 POST
  async function uploadPlanilhas(files) {
    try {
      const allRecords = [];
      const arquivosOk = [];
      const arquivosFalha = [];

      for (const file of files) {
        try {
          const text = await _readFileText(file);
          const recs = parseExportedHtml(text, file.name);
          if (recs.length) {
            allRecords.push(...recs);
            arquivosOk.push(`${file.name} (${recs.length})`);
          } else {
            arquivosFalha.push(file.name);
          }
        } catch (err) {
          arquivosFalha.push(`${file.name} (erro: ${err.message})`);
        }
      }

      if (!allRecords.length) {
        let msg = `Nenhuma vistoria encontrada nos ${arquivosFalha.length} arquivo(s).\n\n`;
        msg += "Verifique se são arquivos exportados do Sispevi (formato HTML).\n\n";
        if (_debugHeaders.length) {
          msg += "Headers detectados (primeiras tabelas):\n";
          msg += _debugHeaders.slice(0, 3).map((h) => "• " + h).join("\n");
          msg += "\n\nEsperado: DATA | MODELO | PLACA | CLIENTE | SERVICO | FP | PRECO";
          msg += "\n(ou minimo: DATA | PLACA | SERVICO | VALOR)";
          console.log("[planilha-dia] Headers detectados:", _debugHeaders);
        }
        alert(msg);
        return;
      }

      // Salva como data de HOJE — vistorias com data diferente ganham flag
      // dia_anterior automaticamente no GET /status
      const dataIso = _todayIso();
      const arquivosLabel = arquivosOk.join(" + ");

      const csrf = await _csrf();
      const r = await fetch(`${apiBase}/api/planilha/upload`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf },
        body: JSON.stringify({ data: dataIso, arquivo: arquivosLabel, records: allRecords }),
      });
      const j = await r.json();
      if (!j.success) {
        alert("Falha no upload: " + (j.error || "desconhecida"));
        return;
      }

      // Monta resumo do upload
      const linhas = [
        `✓ ${allRecords.length} vistoria(s) importada(s)`,
        `Arquivos: ${arquivosOk.length}/${files.length}`,
      ];
      if (arquivosFalha.length) {
        linhas.push(`Falharam: ${arquivosFalha.slice(0, 3).join(", ")}`);
      }
      // Aviso se placas sumiram desde o upload anterior (decisão 3 — diff inteligente)
      if (Array.isArray(j.placas_removidas) && j.placas_removidas.length > 0) {
        linhas.push("");
        linhas.push(`⚠ ${j.placas_removidas.length} placa(s) sumiu/sumiram da versão anterior:`);
        linhas.push(j.placas_removidas.slice(0, 10).join(", ") +
                    (j.placas_removidas.length > 10 ? `, …+${j.placas_removidas.length - 10}` : ""));
      }
      if (arquivosFalha.length || j.placas_removidas?.length > 0) {
        alert(linhas.join("\n"));
      }
      await fetchStatus();
    } catch (e) {
      alert("Erro ao processar planilha(s): " + e.message);
    }
  }

  // Wire
  els.btnUpload.addEventListener("click", () => els.fileInput.click());
  els.fileInput.addEventListener("change", (e) => {
    const files = Array.from(e.target.files || []);
    if (files.length) uploadPlanilhas(files);
    e.target.value = "";
  });
  els.btnRefresh.addEventListener("click", () => fetchStatus({ silent: false }));
  els.toggleDetails.addEventListener("click", () => {
    _detailsOpen = !_detailsOpen;
    if (_lastStatus) renderStatus(_lastStatus);
  });

  // ─── Auto-refresh inteligente ────────────────────────────────────────
  // 2min, mas só dispara quando aba está visível. Quando aba volta a ser
  // visível depois de 2min escondida, dispara um refresh imediato.
  const REFRESH_INTERVAL_MS = 120_000; // 2min
  let _autoRefreshTimer = null;
  let _lastFetchAt = Date.now();

  function _scheduleAutoRefresh() {
    if (_autoRefreshTimer) clearInterval(_autoRefreshTimer);
    _autoRefreshTimer = setInterval(() => {
      if (document.visibilityState !== "visible") return; // pausa em background
      fetchStatus({ silent: true }).then(() => { _lastFetchAt = Date.now(); });
    }, REFRESH_INTERVAL_MS);
  }

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      const elapsed = Date.now() - _lastFetchAt;
      if (elapsed > REFRESH_INTERVAL_MS) {
        // estava escondida tempo suficiente — refresh imediato
        fetchStatus({ silent: true }).then(() => { _lastFetchAt = Date.now(); });
      }
    }
  });

  // Listener: lançamento criado no PDV → refresh imediato pra item sumir
  // da lista de "AV sem PDV" (cruzou agora) sem operadora ter que esperar
  // os 2min do auto-refresh
  window.addEventListener("caixa:lancamento-criado", () => {
    fetchStatus({ silent: true }).then(() => { _lastFetchAt = Date.now(); });
  });

  // Carga inicial + agenda auto-refresh
  fetchStatus().then(() => {
    _lastFetchAt = Date.now();
    _scheduleAutoRefresh();
  });
})();
