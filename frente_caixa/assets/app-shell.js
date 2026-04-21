/* ============================================================================
   Astro Vistorias — App Shell JS
   Injeta sidebar + appbar nas telas de gestao. Uso:

     <div id="app-shell"></div>
     <script src="/assets/app-shell.js"></script>
     <script>
       AstroShell.init({
         active: "master",             // id do item ativo
         title: "Painel Master",       // titulo da pagina (appbar)
         breadcrumbs: [{label:"Rede"},{label:"Unidades"}],  // opcional
         unit: { slug: "barueri", nome: "Barueri" },         // opcional
         date: true                    // exibe data de hoje no appbar
       });
     </script>
   ============================================================================ */

(function (global) {
  "use strict";

  const STORAGE_KEY = "astro.shell.collapsed";
  const THEME_KEY   = "astro.shell.theme";

  // ── Tema (light/dark) ──────────────────────────────────────
  function readTheme() {
    try {
      const saved = localStorage.getItem(THEME_KEY);
      if (saved === "dark" || saved === "light") return saved;
    } catch (_) {}
    return (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) ? "dark" : "light";
  }
  function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem(THEME_KEY, theme); } catch (_) {}
    const btn = document.getElementById("abThemeToggle");
    if (btn) {
      btn.innerHTML = theme === "dark" ? ICONS.sun : ICONS.moon;
      btn.setAttribute("title", theme === "dark" ? "Tema claro" : "Tema escuro");
      btn.setAttribute("aria-label", theme === "dark" ? "Alternar para tema claro" : "Alternar para tema escuro");
    }
  }
  // Aplica ASAP (antes do shell ser injetado) para evitar flash
  (function preApplyTheme() {
    try {
      const saved = localStorage.getItem(THEME_KEY);
      const theme = (saved === "dark" || saved === "light")
        ? saved
        : ((window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) ? "dark" : "light");
      document.documentElement.setAttribute("data-theme", theme);
    } catch (_) {}
  })();

  // ── Icon set (line, 1.75 stroke, 20x20) ─────────────────────
  const _svg = (paths) => `<svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round">${paths}</svg>`;
  const ICONS = {
    home:        _svg('<path d="M3 9.5 12 3l9 6.5V20a1 1 0 0 1-1 1h-5v-7h-6v7H4a1 1 0 0 1-1-1z"/>'),
    caixa:       _svg('<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M2 10h20"/><path d="M6 15h4"/>'),
    fechamento:  _svg('<rect x="4" y="4" width="16" height="16" rx="2"/><path d="m9 12 2 2 4-4"/>'),
    gerencial:   _svg('<path d="M3 20V10"/><path d="M9 20V4"/><path d="M15 20v-8"/><path d="M21 20V7"/>'),
    relatorios:  _svg('<path d="M3 17l6-6 4 4 8-8"/><path d="M14 7h7v7"/>'),
    master:      _svg('<rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/>'),
    unidades:    _svg('<path d="M3 9V21h18V9"/><path d="M3 9l2-5h14l2 5"/><path d="M9 21v-5h6v5"/>'),
    usuarios:    _svg('<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>'),
    usuariosCog: _svg('<circle cx="9" cy="8" r="4"/><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="18" cy="15" r="2.5"/><path d="M18 11.5v1.2M18 17.3v1.2M21.1 13.2l-1 .6M14.9 16.2l-1 .6M21.1 16.8l-1-.6M14.9 13.8l-1-.6"/>'),
    manual:      _svg('<path d="M4 4v16a2 2 0 0 1 2-2h14V2H6a2 2 0 0 0-2 2z"/><path d="M20 18v4H6a2 2 0 0 1-2-2"/>'),
    historico:   _svg('<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l3 2"/>'),
    config:      _svg('<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.6 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>'),
    globe:       _svg('<circle cx="12" cy="12" r="9"/><path d="M3 12h18"/><path d="M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/>'),
    mail:        _svg('<rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 7 9 6 9-6"/>'),
    key:         _svg('<circle cx="8" cy="15" r="4"/><path d="m10.85 12.15 10-10"/><path d="m18 5 3 3"/><path d="m15 8 3 3"/>'),
    shield:      _svg('<path d="M12 2 4 6v6c0 5 3.5 9 8 10 4.5-1 8-5 8-10V6z"/><path d="m9 12 2 2 4-4"/>'),
    logout:      _svg('<path d="M10 17l-5-5 5-5"/><path d="M5 12h12"/><path d="M21 5v14"/>'),
    search:      _svg('<circle cx="11" cy="11" r="7"/><path d="m21 21-4.35-4.35"/>'),
    moon:        _svg('<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z"/>'),
    sun:         _svg('<circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m4.93 19.07 1.41-1.41"/><path d="m17.66 6.34 1.41-1.41"/>'),
    wrench:      _svg('<path d="M14.7 6.3a4 4 0 0 0 5.66 5.66l-9.19 9.19a2.83 2.83 0 0 1-4-4z"/><path d="m18 2-3.5 3.5"/>'),
    download:    _svg('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><path d="m7 10 5 5 5-5"/><path d="M12 15V3"/>'),
    send:        _svg('<path d="m22 2-7 20-4-9-9-4 20-7z"/><path d="M22 2 11 13"/>'),
    edit:        _svg('<path d="M12 20h9"/><path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4z"/>'),
    trash:       _svg('<path d="M3 6h18"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M10 11v6M14 11v6"/>'),
    plus:        _svg('<path d="M12 5v14M5 12h14"/>'),
    cloud:       _svg('<path d="M17.5 19a4.5 4.5 0 1 0-.88-8.91 5.5 5.5 0 0 0-10.6 1.75A4 4 0 0 0 6.5 19z"/>'),
    cloudDown:   _svg('<path d="M20 16.2A4.5 4.5 0 0 0 17.5 8h-1.79A7 7 0 1 0 4 14.9"/><path d="m8 17 4 4 4-4"/><path d="M12 12v9"/>'),
  };
  const renderIcon = (key) => ICONS[key] || `<span class="sb-item-emoji">•</span>`;

  // ── Catalogo de itens de navegacao ─────────────────────────
  //   requires: "master" | "gerencial" | "unit" | null
  //   Items que exigem contexto de unidade recebem apiBase concatenado
  const NAV_CATALOG = [
    {
      group: "Operação",
      requires: "unit",
      items: [
        { id: "caixa",       label: "Caixa do Dia", icon: "caixa",      hrefUnit: "/caixa2" },
        { id: "fechamento",  label: "Fechamento",   icon: "fechamento", hrefUnit: "/fechamento" },
        { id: "historico",   label: "Histórico",    icon: "historico",  hrefUnit: "/historico" },
      ],
    },
    {
      group: "Gestão",
      items: [
        { id: "gerencial-unit", label: "Gerencial",   icon: "gerencial",  hrefUnit: "/gerencial", requires: "gerencial" },
        { id: "relatorios",     label: "Relatórios",  icon: "relatorios", disabled: true, badge: "Em breve" },
      ],
    },
    {
      group: "Rede",
      requires: "master",
      items: [
        { id: "master",           label: "Painel Master",    icon: "master",     href: "/master" },
        { id: "gerencial-rede",   label: "Gerencial Rede",   icon: "gerencial",  href: "/gerencial" },
        { id: "historico-caixa",     label: "Histórico PDV",        icon: "historico",  href: "/gerencial/historico-caixa" },
        { id: "usuarios-conectados", label: "Usuários Conectados",  icon: "usuarios",   href: "/master/usuarios-conectados" },
        { id: "usuarios",            label: "Usuários",             icon: "usuariosCog", href: "/master/usuarios" },
        { id: "unidades",            label: "Unidades",             icon: "unidades",   disabled: true, badge: "Em breve" },
      ],
    },
    {
      group: "Sistema",
      items: [
        { id: "manual",        label: "Manual",         icon: "manual", hrefUnit: "/manual", href: "/manual" },
        { id: "configuracoes", label: "Configurações",  icon: "config", disabled: true, badge: "Em breve" },
      ],
    },
    {
      group: "Astro na web",
      items: [
        { id: "site",    label: "astrovistorias.com.br", icon: "globe",  href: "https://www.astrovistorias.com.br", external: true },
        { id: "webmail", label: "Webmail corporativo",   icon: "mail",   href: "https://mailpro.uol.com.br/astrovistorias.com.br", external: true },
        { id: "oxxy",    label: "Oxxy · Sispevi",        icon: "key",    href: "https://v6.sispevi.com.br/Login.aspx", external: true },
        { id: "mega",    label: "Vistoria Cautelar",     icon: "shield", href: "https://v6.megalaudo.com.br/Login.aspx?ReturnUrl=%2fDefault.aspx", external: true },
      ],
    },
  ];

  const APP_VERSION = "v2.1 · Painel Astro";

  // ── Helpers ─────────────────────────────────────────────────
  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
  }

  function initials(name) {
    if (!name) return "U";
    const parts = String(name).trim().split(/\s+/);
    if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
    return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
  }

  function todayLabel() {
    const d = new Date().toLocaleDateString("pt-BR", {
      weekday: "long", day: "2-digit", month: "long"
    });
    return d.charAt(0).toUpperCase() + d.slice(1);
  }

  function detectUnitSlug() {
    const m = location.pathname.match(/^\/u\/([^\/]+)/);
    return m ? m[1] : null;
  }

  // ── Fetch do usuario logado + unidades ─────────────────────
  async function loadMe() {
    try {
      const res = await fetch("/api/me", { credentials: "same-origin" });
      if (!res.ok) return {};
      return await res.json();
    } catch (_) { return {}; }
  }

  async function loadUnitInfo(slug) {
    if (!slug) return {};
    try {
      const res = await fetch(`/u/${slug}/api/info`, { credentials: "same-origin" });
      if (!res.ok) return {};
      return await res.json();
    } catch (_) { return {}; }
  }

  // ── Filtra grupos conforme permissoes + contexto ───────────
  function resolveNav(ctx) {
    const slug = ctx.unitSlug;
    const apiBase = slug ? `/u/${slug}` : "";
    const out = [];

    for (const group of NAV_CATALOG) {
      // Filtro do grupo
      if (group.requires === "master" && !ctx.isMaster) continue;
      if (group.requires === "unit" && !slug) continue;

      const items = [];
      for (const it of group.items) {
        // Filtro por permissao
        if (it.requires === "gerencial" && !ctx.isGerencial) continue;
        if (it.requires === "master" && !ctx.isMaster) continue;

        // Resolve href conforme contexto
        let href = it.href || null;
        if (it.hrefUnit && slug) href = apiBase + it.hrefUnit;
        else if (it.hrefUnit && !it.href) continue; // sem unidade e sem fallback

        items.push({ ...it, href });
      }
      if (items.length) out.push({ label: group.group, items });
    }
    return out;
  }

  // ── Render HTML ────────────────────────────────────────────
  function renderSidebar(nav, ctx, config) {
    const activeId = config.active;

    const groupsHtml = nav.map(g => {
      const itemsHtml = g.items.map(it => {
        const classes = ["sb-item"];
        if (it.id === activeId) classes.push("is-active");
        if (it.disabled) classes.push("is-disabled");
        if (it.danger) classes.push("is-danger");
        if (it.external) classes.push("is-external");
        const tag = it.disabled || !it.href ? "span" : "a";
        const hrefAttr = it.href && !it.disabled ? ` href="${esc(it.href)}"` : "";
        const targetAttr = it.external ? ` target="_blank" rel="noopener"` : "";
        const badgeHtml = it.badge ? `<span class="sb-item-badge">${esc(it.badge)}</span>`
                         : it.external ? `<span class="sb-item-external">↗</span>` : "";
        return `
          <${tag} class="${classes.join(" ")}"${hrefAttr}${targetAttr}>
            <span class="sb-item-icon">${renderIcon(it.icon)}</span>
            <span class="sb-item-label">${esc(it.label)}</span>
            ${badgeHtml}
          </${tag}>`;
      }).join("");
      return `
        <div class="sb-group">
          <div class="sb-group-title">${esc(g.label)}</div>
          ${itemsHtml}
        </div>`;
    }).join("");

    // Rodape: usuario + logout
    const userName = ctx.userName || "Usuário";
    const userRole = ctx.isMaster ? "Administrador"
                    : ctx.unitSlug ? `Unidade ${ctx.unitNome || ctx.unitSlug}`
                    : "Operador";

    return `
      <aside class="app-sidebar">
        <div class="sb-brand">
          <a class="sb-brand-logo" href="/home" aria-label="Astro Vistorias — Início">
            <img src="/logo/PNG/ASTRO%20Logo_Full%20Color.png" alt="Astro Vistorias">
          </a>
        </div>
        <nav class="sb-nav">
          ${groupsHtml}
          <div class="sb-group">
            <a class="sb-item is-danger" href="/logout">
              <span class="sb-item-icon">${renderIcon("logout")}</span>
              <span class="sb-item-label">Sair</span>
            </a>
          </div>
        </nav>
        <div class="sb-footer">
          <div class="sb-user">
            <div class="sb-user-avatar">${esc(initials(userName))}</div>
            <div class="sb-user-text">
              <div class="sb-user-name">${esc(userName)}</div>
              <div class="sb-user-role">${esc(userRole)}</div>
            </div>
          </div>
          <div class="sb-version">${esc(APP_VERSION)}</div>
        </div>
      </aside>`;
  }

  function renderAppbar(ctx, config) {
    const crumbs = config.breadcrumbs || [];
    if (!crumbs.length && config.title) crumbs.push({ label: config.title, current: true });
    const crumbsHtml = crumbs.map((c, i) => {
      const isLast = i === crumbs.length - 1;
      const cls = "ab-crumb" + (isLast || c.current ? " is-current" : "");
      const body = c.href && !isLast
        ? `<a class="${cls}" href="${esc(c.href)}">${esc(c.label)}</a>`
        : `<span class="${cls}">${esc(c.label)}</span>`;
      const sep = i < crumbs.length - 1 ? `<span class="ab-crumb-sep">/</span>` : "";
      return body + sep;
    }).join("");

    const unitChip = ctx.unitNome
      ? `<span class="ab-unit-chip">${esc(ctx.unitNome)}</span>`
      : "";

    const dateChip = config.date
      ? `<span class="ab-date">${esc(todayLabel())}</span>`
      : "";

    const isMac = /Mac|iPhone|iPad/.test(navigator.platform);
    const kbd = isMac ? "⌘K" : "Ctrl K";

    return `
      <header class="app-appbar">
        <button class="ab-toggle" type="button" id="abToggle" aria-label="Alternar menu">☰</button>
        <div class="ab-breadcrumbs">${crumbsHtml}</div>
        <div class="ab-right">
          <button class="ab-cmdk" type="button" id="abCmdK" title="Busca rápida">
            <span class="ab-cmdk-icon">${renderIcon("search")}</span>
            <span class="ab-cmdk-text">Buscar</span>
            <span class="ab-cmdk-kbd">${esc(kbd)}</span>
          </button>
          <button class="ab-icon-btn" type="button" id="abThemeToggle" title="Alternar tema" aria-label="Alternar tema"></button>
          ${dateChip}
          ${unitChip}
        </div>
      </header>`;
  }

  // ── Command Palette ────────────────────────────────────────
  function renderCommandPalette() {
    return `
      <div class="cmdk" id="cmdkOverlay" aria-hidden="true">
        <div class="cmdk-backdrop" data-cmdk-close="1"></div>
        <div class="cmdk-panel" role="dialog" aria-modal="true" aria-label="Busca rápida">
          <div class="cmdk-input-wrap">
            <span class="cmdk-input-icon">${renderIcon("search")}</span>
            <input type="text" class="cmdk-input" id="cmdkInput" placeholder="Buscar unidade, tela ou ação..." autocomplete="off">
            <span class="cmdk-input-esc">esc</span>
          </div>
          <div class="cmdk-results" id="cmdkResults"></div>
          <div class="cmdk-hint">
            <span><kbd>↑</kbd><kbd>↓</kbd> navegar</span>
            <span><kbd>↵</kbd> abrir</span>
            <span><kbd>esc</kbd> fechar</span>
          </div>
        </div>
      </div>`;
  }

  async function buildCommandIndex(ctx) {
    const idx = [];

    // Telas gerais
    if (ctx.isMaster) {
      idx.push({ title: "Painel Master", subtitle: "Visão geral da rede", icon: "master", href: "/master", group: "Telas" });
      idx.push({ title: "Gerencial Rede", subtitle: "Relatórios consolidados", icon: "gerencial", href: "/gerencial", group: "Telas" });
    }
    idx.push({ title: "Início", subtitle: "Centro de comando", icon: "home", href: "/home", group: "Telas" });

    // Unidades (via API master)
    if (ctx.isMaster) {
      try {
        const res = await fetch("/master/api/units", { credentials: "same-origin" });
        if (res.ok) {
          const data = await res.json();
          (data.units || []).forEach(u => {
            idx.push({ title: `Caixa do Dia · ${u.nome}`,  subtitle: `PDV em tempo real`,        icon: "caixa",      href: `/u/${u.id}/caixa2`,      group: "Unidades" });
            idx.push({ title: `Fechamento · ${u.nome}`,    subtitle: `Envio ao Tiny`,            icon: "fechamento", href: `/u/${u.id}/fechamento`,  group: "Unidades" });
            idx.push({ title: `Gerencial · ${u.nome}`,     subtitle: `Relatórios da unidade`,    icon: "gerencial",  href: `/u/${u.id}/gerencial`,   group: "Unidades" });
          });
        }
      } catch (_) {}
    } else if (ctx.unitSlug) {
      const base = `/u/${ctx.unitSlug}`;
      const nome = ctx.unitNome || ctx.unitSlug;
      idx.push({ title: "Caixa do Dia",  subtitle: nome, icon: "caixa",      href: `${base}/caixa2`,     group: "Operação" });
      idx.push({ title: "Fechamento",    subtitle: nome, icon: "fechamento", href: `${base}/fechamento`, group: "Operação" });
      if (ctx.isGerencial) {
        idx.push({ title: "Gerencial",   subtitle: nome, icon: "gerencial",  href: `${base}/gerencial`,  group: "Operação" });
      }
    }

    // Links externos
    idx.push({ title: "astrovistorias.com.br",  subtitle: "Site oficial",                     icon: "globe",  href: "https://www.astrovistorias.com.br", group: "Web", external: true });
    idx.push({ title: "Webmail corporativo",    subtitle: "mailpro.uol.com.br",               icon: "mail",   href: "https://mailpro.uol.com.br/astrovistorias.com.br", group: "Web", external: true });
    idx.push({ title: "Oxxy · Sispevi",         subtitle: "Acesso ao sistema Oxxy",           icon: "key",    href: "https://v6.sispevi.com.br/Login.aspx", group: "Web", external: true });
    idx.push({ title: "Vistoria Cautelar",      subtitle: "Megalaudo · v6.megalaudo.com.br",  icon: "shield", href: "https://v6.megalaudo.com.br/Login.aspx?ReturnUrl=%2fDefault.aspx", group: "Web", external: true });

    // Sistema
    idx.push({ title: "Manual",  subtitle: "Como usar o sistema", icon: "manual", href: "/manual", group: "Sistema" });
    idx.push({ title: "Sair",    subtitle: "Encerrar sessão",     icon: "logout", href: "/logout", group: "Sistema" });

    return idx;
  }

  function fuzzyMatch(q, text) {
    if (!q) return 0;
    const t = text.toLowerCase();
    if (t.includes(q)) return 100 - t.indexOf(q);
    // token match
    const tokens = q.split(/\s+/).filter(Boolean);
    let score = 0;
    for (const tok of tokens) {
      if (t.includes(tok)) score += 10; else return 0;
    }
    return score;
  }

  function wireCommandPalette(ctx) {
    const overlay = document.getElementById("cmdkOverlay");
    const input   = document.getElementById("cmdkInput");
    const results = document.getElementById("cmdkResults");
    const btn     = document.getElementById("abCmdK");
    if (!overlay || !input || !results) return;

    let commands = [];
    let filtered = [];
    let active   = 0;
    let loaded   = false;

    async function ensureIndex() {
      if (loaded) return;
      loaded = true;
      commands = await buildCommandIndex(ctx);
    }

    async function open() {
      await ensureIndex();
      overlay.classList.add("is-open");
      overlay.setAttribute("aria-hidden", "false");
      input.value = "";
      render("");
      setTimeout(() => input.focus(), 0);
    }

    function close() {
      overlay.classList.remove("is-open");
      overlay.setAttribute("aria-hidden", "true");
    }

    function render(q) {
      q = (q || "").trim().toLowerCase();
      if (!q) {
        filtered = commands.slice(0, 12);
      } else {
        filtered = commands
          .map(c => ({ c, s: fuzzyMatch(q, `${c.title} ${c.subtitle || ""} ${c.group}`) }))
          .filter(x => x.s > 0)
          .sort((a,b) => b.s - a.s)
          .slice(0, 20)
          .map(x => x.c);
      }
      active = 0;
      if (!filtered.length) {
        results.innerHTML = `<div class="cmdk-empty">Nada encontrado para <strong>${esc(q)}</strong></div>`;
        return;
      }
      let lastGroup = "";
      const html = filtered.map((c, i) => {
        const header = c.group !== lastGroup ? `<div class="cmdk-group">${esc(c.group)}</div>` : "";
        lastGroup = c.group;
        return header + `
          <div class="cmdk-item ${i === 0 ? 'is-active' : ''}" data-idx="${i}">
            <span class="cmdk-item-icon">${renderIcon(c.icon)}</span>
            <div class="cmdk-item-body">
              <div class="cmdk-item-title">${esc(c.title)}${c.external ? ' <span class="cmdk-ext">↗</span>' : ''}</div>
              ${c.subtitle ? `<div class="cmdk-item-sub">${esc(c.subtitle)}</div>` : ""}
            </div>
            <span class="cmdk-item-enter">↵</span>
          </div>`;
      }).join("");
      results.innerHTML = html;
    }

    function setActive(idx) {
      const items = results.querySelectorAll(".cmdk-item");
      if (!items.length) return;
      active = (idx + items.length) % items.length;
      items.forEach((el, i) => el.classList.toggle("is-active", i === active));
      items[active].scrollIntoView({ block: "nearest" });
    }

    function executeActive() {
      const c = filtered[active];
      if (!c) return;
      close();
      if (c.external) window.open(c.href, "_blank", "noopener");
      else window.location.href = c.href;
    }

    // Triggers
    if (btn) btn.addEventListener("click", open);
    document.addEventListener("keydown", (e) => {
      const isOpen = overlay.classList.contains("is-open");
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        isOpen ? close() : open();
        return;
      }
      if (!isOpen) return;
      if (e.key === "Escape") { e.preventDefault(); close(); }
      else if (e.key === "ArrowDown") { e.preventDefault(); setActive(active + 1); }
      else if (e.key === "ArrowUp")   { e.preventDefault(); setActive(active - 1); }
      else if (e.key === "Enter")     { e.preventDefault(); executeActive(); }
    });

    input.addEventListener("input", (e) => render(e.target.value));

    overlay.addEventListener("click", (e) => {
      if (e.target.dataset && e.target.dataset.cmdkClose) close();
      const item = e.target.closest(".cmdk-item");
      if (item) {
        active = parseInt(item.dataset.idx, 10) || 0;
        executeActive();
      }
    });
  }

  // ── Interacao (collapse toggle, mobile open/close) ─────────
  function wireInteractions(shellEl) {
    const btn = document.getElementById("abToggle");
    if (!btn) return;

    btn.addEventListener("click", () => {
      const isMobile = window.matchMedia("(max-width: 1100px)").matches;
      if (isMobile) {
        shellEl.classList.toggle("is-open");
      } else {
        const next = !shellEl.classList.contains("is-collapsed");
        shellEl.classList.toggle("is-collapsed", next);
        try { localStorage.setItem(STORAGE_KEY, next ? "1" : "0"); } catch (_) {}
      }
    });

    // Fecha off-canvas ao clicar no backdrop
    const backdrop = shellEl.querySelector(".app-backdrop");
    if (backdrop) backdrop.addEventListener("click", () => shellEl.classList.remove("is-open"));

    // Fecha off-canvas ao clicar em item de nav (mobile)
    shellEl.querySelectorAll(".sb-item").forEach(el => {
      el.addEventListener("click", () => {
        if (window.matchMedia("(max-width: 1100px)").matches) {
          shellEl.classList.remove("is-open");
        }
      });
    });
  }

  // ── Init ───────────────────────────────────────────────────
  async function init(config) {
    config = config || {};
    const mount = document.getElementById("app-shell");
    if (!mount) { console.warn("AstroShell: #app-shell nao encontrado"); return; }

    // Descobre contexto (usuario + unidade)
    const slug = detectUnitSlug();
    const [me, unitInfo] = await Promise.all([loadMe(), loadUnitInfo(slug)]);

    const ctx = {
      unitSlug:    slug,
      unitNome:    unitInfo.unidade || (config.unit && config.unit.nome) || null,
      userName:    me.usuario || "",
      isMaster:    !!me.master,
      isGerencial: !!(me.gerencial || me.master),
    };

    // Override via config
    if (config.unit && config.unit.nome) ctx.unitNome = config.unit.nome;

    // Resolve nav
    const nav = resolveNav(ctx);

    // Monta HTML do shell
    const sidebarHtml = renderSidebar(nav, ctx, config);
    const appbarHtml = renderAppbar(ctx, config);

    // Envolve o conteudo existente em .app-main
    // Estrutura esperada no DOM: <div id="app-shell"></div><main id="app-main">...</main>
    const mainEl = document.getElementById("app-main");
    if (!mainEl) {
      console.warn("AstroShell: #app-main nao encontrado — adicione um <main id='app-main'> envolvendo o conteudo");
    }

    // Aplica wrapper: #app-shell vira container do layout
    mount.classList.add("app-shell");

    // Collapsed preference
    try {
      if (localStorage.getItem(STORAGE_KEY) === "1") mount.classList.add("is-collapsed");
    } catch (_) {}

    // Opcao de tema
    if (config.theme === "light") mount.classList.add("theme-light");

    // Move o main pra dentro do shell se ainda nao estiver
    if (mainEl && mainEl.parentNode !== mount) {
      mount.appendChild(mainEl);
    }

    // Injeta sidebar antes do main; appbar dentro do main como primeiro filho
    mount.insertAdjacentHTML("afterbegin", sidebarHtml);
    mount.insertAdjacentHTML("beforeend", `<div class="app-backdrop"></div>`);

    if (mainEl) {
      mainEl.classList.add("app-main");
      mainEl.insertAdjacentHTML("afterbegin", appbarHtml);
    }

    // Command palette (Ctrl/Cmd + K)
    document.body.insertAdjacentHTML("beforeend", renderCommandPalette());

    wireInteractions(mount);
    wireCommandPalette(ctx);

    // Tema — aplica ícone inicial e wire do toggle
    applyTheme(readTheme());
    const themeBtn = document.getElementById("abThemeToggle");
    if (themeBtn) {
      themeBtn.addEventListener("click", () => {
        const curr = document.documentElement.getAttribute("data-theme") || "light";
        applyTheme(curr === "dark" ? "light" : "dark");
      });
    }
  }

  global.AstroShell = { init, icon: (key) => ICONS[key] || "" };
})(window);
