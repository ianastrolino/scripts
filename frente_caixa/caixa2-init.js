document.addEventListener("DOMContentLoaded", () => {
  const FP_LABELS = { dinheiro: "💵 Dinheiro", debito: "💳 Débito", credito: "💳 Crédito", pix: "⚡ PIX", faturado: "🧾 Faturado" };
  const brlFmt = v => "R$ " + Number(v).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  // Redireciona clique do btnLancar para abrir modal de confirmação
  const btnLancar = document.getElementById("btnLancar");
  btnLancar.removeEventListener("click", lancar);
  btnLancar.addEventListener("click", () => {
    btnLancar.disabled = true;
    const placa   = document.getElementById("fPlaca").value.trim().toUpperCase();
    const cliente = document.getElementById("fCliente").value.trim();
    const servico = document.getElementById("fServico").value;
    const valor   = parseFloat(document.getElementById("fValor").value) || 0;
    const fp      = state.fpSelecionado;

    document.getElementById("cfPlaca").textContent   = placa;
    document.getElementById("cfCliente").textContent = cliente;
    document.getElementById("cfServico").textContent = servico;
    document.getElementById("cfFp").textContent      = FP_LABELS[fp] || fp;
    document.getElementById("cfValor").textContent   = brlFmt(valor);

    const fpEl = document.getElementById("cfFp");
    fpEl.className = "confirm-row-val fp-chip " + (fp || "");

    document.getElementById("confirmLancarModal").classList.add("open");
    document.getElementById("confirmLancarOkBtn").focus();
  });

  document.getElementById("confirmLancarOkBtn").addEventListener("click", (e) => {
    e.currentTarget.disabled = true;
    document.getElementById("confirmLancarModal").classList.remove("open");
    lancar().finally(() => { e.currentTarget.disabled = false; });
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

  // Rebind FP cards para Layout 2
  document.getElementById("fpGrid").querySelectorAll(".fp-card").forEach(btn => {
    btn.addEventListener("click", () => {
      state.fpSelecionado = btn.dataset.fp;
      document.getElementById("fpGrid").querySelectorAll(".fp-card")
        .forEach(b => b.classList.toggle("selected", b.dataset.fp === btn.dataset.fp));
      validarFormulario();
    });
  });

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
