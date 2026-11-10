// ─────────────────────────────────────────────────────────────
// Lógica del frontend: login OIDC con Keycloak y llamadas al backend
// (a través del API Manager) adjuntando el JWT.
// ─────────────────────────────────────────────────────────────
(function () {
  const cfg = window.APP_CONFIG;
  const out = document.getElementById('out');
  const badge = document.getElementById('authBadge');
  const userInfo = document.getElementById('userInfo');
  const loginBtn = document.getElementById('loginBtn');
  const logoutBtn = document.getElementById('logoutBtn');
  const tokenBtn = document.getElementById('tokenBtn');
  const callBtns = Array.from(document.querySelectorAll('.call'));
  const metricsBtn = document.getElementById('metricsBtn');
  const metricsPre = document.getElementById('metrics');
  let metricsTimer = null;

  const keycloak = new Keycloak({ url: cfg.keycloakUrl, realm: cfg.realm, clientId: cfg.clientId });

  function setLoggedIn(logged) {
    badge.textContent = logged ? 'autenticado' : 'sin sesión';
    badge.className = 'badge ' + (logged ? 'ok' : 'off');
    loginBtn.disabled = logged;
    logoutBtn.disabled = !logged;
    tokenBtn.disabled = !logged;
    callBtns.forEach(b => b.disabled = !logged);
    metricsBtn.disabled = !logged;
    if (!logged && metricsTimer) { clearInterval(metricsTimer); metricsTimer = null; metricsBtn.textContent = 'Métricas en vivo ▶'; }
    if (logged) {
      const p = keycloak.tokenParsed || {};
      userInfo.textContent = '· ' + (p.preferred_username || p.email || 'usuario');
    } else {
      userInfo.textContent = '';
    }
  }

  keycloak.init({ onLoad: 'check-sso', pkceMethod: 'S256' })
    .then(authenticated => setLoggedIn(authenticated))
    .catch(() => { out.textContent = 'Error al inicializar Keycloak. Revisa keycloakUrl/realm/clientId en config.js.'; });

  loginBtn.onclick = () => keycloak.login();
  logoutBtn.onclick = () => keycloak.logout();
  tokenBtn.onclick = () => { out.textContent = 'JWT (access token):\n\n' + keycloak.token; };

  callBtns.forEach(btn => {
    btn.onclick = async () => {
      const path = btn.getAttribute('data-path');
      const body = btn.getAttribute('data-body');
      out.textContent = 'Llamando ' + path + ' …';
      try {
        // Refresca el token si está por expirar, y adjúntalo.
        await keycloak.updateToken(30).catch(() => {});
        const res = await fetch(cfg.apimUrl + path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + keycloak.token },
          body: body
        });
        const text = await res.text();
        let pretty = text;
        try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch (e) {}
        out.textContent = 'HTTP ' + res.status + ' ' + res.statusText + '\n\n' + pretty;
      } catch (err) {
        out.textContent = 'Error de red: ' + err + '\n(¿apimUrl correcto? ¿CORS habilitado en el API Manager?)';
      }
    };
  });

  // Métricas Kafka en tiempo real (polling cada 2 s mientras está activo).
  function esc(s) { return String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])); }
  async function fetchMetrics() {
    try {
      await keycloak.updateToken(30).catch(() => {});
      const res = await fetch(cfg.apimUrl + '/api/kafka/metrics', {
        headers: { 'Authorization': 'Bearer ' + keycloak.token }
      });
      if (!res.ok) { metricsPre.innerHTML = '<span class="muted">HTTP ' + res.status + ' — ¿sesión válida?</span>'; return; }
      const m = await res.json();
      let h = '<div>';
      h += '<span class="kstat">Ocupación global <b>' + (m.ocupacionGlobalPorcentaje || 0) + '%</b></span>';
      h += '<span class="kstat">Ingresos <b>$' + (m.ingresosAcumulados || 0) + '</b></span>';
      h += '<span class="kstat">Eventos <b>' + ((m.eventosOcupacionProcesados || 0) + (m.eventosPagoProcesados || 0)) + '</b></span></div>';
      const zonas = m.ocupacionPorZona || {};
      Object.keys(zonas).forEach(z => {
        const pct = zonas[z].porcentaje || 0;
        h += '<div class="kbar-row"><div class="kbar-label">' + esc(z) + '</div>'
          + '<div class="kbar-track"><div class="kbar-fill" style="width:' + pct + '%"></div></div>'
          + '<div class="kbar-pct">' + pct + '%</div></div>';
      });
      const evs = m.ultimosEventos || [];
      if (evs.length) {
        h += '<table class="kevents"><thead><tr><th>Tipo</th><th>Part.</th><th>Offset</th><th>Detalle</th></tr></thead><tbody>';
        evs.forEach(e => {
          const cls = e.tipo === 'pago' ? 'pay' : 'ocu';
          h += '<tr><td><span class="ktag ' + cls + '">' + esc(e.tipo) + '</span></td>'
            + '<td>' + esc(e.particion) + '</td><td>' + esc(e.offset) + '</td><td>' + esc(e.detalle) + '</td></tr>';
        });
        h += '</tbody></table>';
      }
      metricsPre.innerHTML = h;
    } catch (e) {
      metricsPre.innerHTML = '<span class="muted">Error al leer métricas: ' + esc(e) + '</span>';
    }
  }
  metricsBtn.onclick = () => {
    if (metricsTimer) {
      clearInterval(metricsTimer); metricsTimer = null; metricsBtn.textContent = 'Métricas en vivo ▶';
    } else {
      fetchMetrics(); metricsTimer = setInterval(fetchMetrics, 2000); metricsBtn.textContent = 'Detener ⏸';
    }
  };
})();
