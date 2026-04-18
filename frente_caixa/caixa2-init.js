document.addEventListener("DOMContentLoaded", () => {
  const FP_LABELS = { dinheiro: "💵 Dinheiro", debito: "💳 Débito", credito: "💳 Crédito", pix: "⚡ PIX", faturado: "🧾 Faturado", detran: "🚗 Taxa DETRAN" };
  const brlFmt = v => "R$ " + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function fmtCPF(digits) {
    if (!digits) return "";
    if (digits.length === 11) return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4");
    if (digits.length === 14) return digits.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5");
    return digits;
  }

  const CPF_SERVICES = ["LAUDO DE TRANSFERENCIA", "REVISTORIA"];

  function updateCpfField() {
    const fp = state.fpSelecionado;
    const servico = (document.getElementById("fServico")?.value || "").toUpperCase();
    const wrap = document.getElementById("fCpfWrap");
    if (!wrap) return;
    const show = fp && fp !== "faturado" && CPF_SERVICES.some(s => servico.includes(s));
    wrap.style.display = show ? "" : "none";
    if (!show) {
      const cpfEl = document.getElementById("fCpf");
      if (cpfEl) { cpfEl.value = ""; cpfEl.style.borderColor = ""; }
      const cpfErr = document.getElementById("cpfError");
      if (cpfErr) { cpfErr.textContent = ""; cpfErr.style.display = "none"; }
    }
    validarFormulario();
  }

  // Redireciona clique do btnLancar para abrir modal de confirmação
  const btnLancar = document.getElementById("btnLancar");
  btnLancar.removeEventListener("click", lancar);
  btnLancar.addEventListener("click", () => {
    btnLancar.disabled = true;
    const placa   = document.getElementById("fPlaca").value.trim().toUpperCase();
    const cliente = document.getElementById("fCliente").value.trim().toUpperCase();
    const cpfRaw  = (document.getElementById("fCpf")?.value || "").replace(/\D/g, "");
    const servico = document.getElementById("fServico").value;
    const valor   = parseFloat(document.getElementById("fValor").value) || 0;
    const fp      = state.fpSelecionado;

    document.getElementById("cfPlaca").textContent   = placa;
    document.getElementById("cfCliente").textContent = cliente;
    document.getElementById("cfServico").textContent = servico;
    document.getElementById("cfFp").textContent      = FP_LABELS[fp] || fp;
    document.getElementById("cfValor").textContent   = brlFmt(valor);

    const cfCpfRow = document.getElementById("cfCpfRow");
    const cfCpf    = document.getElementById("cfCpf");
    if (cfCpfRow && cfCpf) {
      if (cpfRaw) {
        cfCpf.textContent = fmtCPF(cpfRaw);
        cfCpfRow.style.display = "";
      } else {
        cfCpfRow.style.display = "none";
      }
    }

    const fpEl = document.getElementById("cfFp");
    fpEl.className = "confirm-row-val fp-chip " + (fp || "");

    document.getElementById("confirmLancarModal").classList.add("open");
    document.getElementById("confirmLancarOkBtn").focus();
  });

  document.getElementById("confirmLancarOkBtn").addEventListener("click", (e) => {
    const okBtn = e.currentTarget;
    okBtn.disabled = true;
    document.getElementById("confirmLancarModal").classList.remove("open");
    lancar().finally(() => { okBtn.disabled = false; });
  });

  document.getElementById("confirmLancarCancelBtn").addEventListener("click", () => {
    document.getElementById("confirmLancarModal").classList.remove("open");
    document.getElementById("btnLancar").disabled = false;
  });

  document.getElementById("confirmLancarModal").addEventListener("click", e => {
    if (e.target === e.currentTarget) {
      e.currentTarget.classList.remove("open");
      document.getElementById("btnLancar").disabled = false;
    }
  });

  document.getElementById("confirmLancarModal").addEventListener("keydown", e => {
    if (e.key === "Enter") { e.preventDefault(); document.getElementById("confirmLancarOkBtn").click(); }
    if (e.key === "Escape") document.getElementById("confirmLancarCancelBtn").click();
  });

  // Rebind FP cards para Layout 2 — mostra/oculta CPF conforme FP
  document.getElementById("fpGrid").querySelectorAll(".fp-card").forEach(btn => {
    btn.addEventListener("click", () => {
      state.fpSelecionado = btn.dataset.fp;
      document.getElementById("fpGrid").querySelectorAll(".fp-card")
        .forEach(b => b.classList.toggle("selected", b.dataset.fp === btn.dataset.fp));
      updateCpfField();
    });
  });

  // Atualiza visibilidade do CPF quando o serviço muda
  document.getElementById("fServico")?.addEventListener("change", updateCpfField);

  document.getElementById("editFpMini").querySelectorAll(".fp-mini").forEach(btn => {
    btn.addEventListener("click", () => {
      state.editFpSelecionado = btn.dataset.fp;
      document.getElementById("editFpMini").querySelectorAll(".fp-mini")
        .forEach(b => b.classList.toggle("sel", b.dataset.fp === btn.dataset.fp));
    });
  });

  // Patch abrirEditar: sincroniza FP mini ao abrir edição
  const _origAbrirEditar = abrirEditar;
  window.abrirEditar = function(id) {
    _origAbrirEditar(id);
    const lc = state.lancamentos.find(l => l.id === id);
    if (!lc) return;
    document.getElementById("editFpMini").querySelectorAll(".fp-mini")
      .forEach(b => b.classList.toggle("sel", b.dataset.fp === lc.fp));
  };
});
