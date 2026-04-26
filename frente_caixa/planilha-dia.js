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
      // Layout típico Sispevi: ... PLACA | CLIENTE_TIPO | SERVICO | ... | FP | VALOR | VALOR
      const cliente = row[placaIdx + 1] || "";
      const servico = row[placaIdx + 2] || "";
      const fp      = detectFp(row, placaIdx + 3);
      const valor   = lastValor(row, placaIdx + 3);

      out.push(makeRecord([dataNorm, "", placaRaw, cliente, servico, fp, valor], idx++, sourceFile));
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
  };
  if (!els.panel) return;

  function _statChip(label, value, cls = "") {
    return `<span class="pd-stat ${cls}"><span>${_escHtml(label)}</span> <strong>${value}</strong></span>`;
  }

  function renderStatus(status) {
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
    const arquivos = status.arquivo ? ` · ${status.arquivo}` : "";
    els.meta.textContent = `v${versao} · atualizada ${upTime}${arquivos}`;
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
  els.btnRefresh.addEventListener("click", fetchStatus);

  fetchStatus();
})();
