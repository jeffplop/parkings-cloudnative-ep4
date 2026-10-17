// ─────────────────────────────────────────────────────────────
// Configuración del frontend. EDITA estos valores según tu entorno.
//   - En LOCAL: usa localhost.
//   - En AZURE: usa la IP pública (o DNS) de tu VM para Keycloak, y la
//     URL de tu API en API Management (APIM) para apimUrl.
// ─────────────────────────────────────────────────────────────
window.APP_CONFIG = {
  // Servidor Keycloak (IDaaS)
  keycloakUrl: "http://localhost:8080",
  realm: "parkings",
  clientId: "parkings-frontend",

  // A dónde se llaman los endpoints del backend:
  //   Local sin APIM  -> "http://localhost:8083"
  //   En Azure con APIM -> "https://<tu-apim>.azure-api.net/parkings"
  apimUrl: "http://localhost:8083"
};
