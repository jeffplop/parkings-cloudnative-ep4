# Parkings Together — Cloud Native (DSY1107 · EP4)

Proyecto de la **Evaluación Parcial N°4** de Desarrollo Cloud Native I. Incorpora
**mensajería con RabbitMQ** (clúster de 2 nodos), **seguridad con IDaaS**
(Keycloak, OAuth 2.0 / OpenID Connect), un **API Manager** que valida el JWT, y un
**frontend** que inicia sesión y consume el backend a través de ese API Manager.
Todo se orquesta con **docker-compose** para ejecutarse en la nube (Microsoft Azure).

**Integrantes:** Jefferson Rojas · Gabriel Molina — **Sección:** 002V

---

## Resumen ejecutivo

El sistema desacopla las tareas de negocio (notificaciones, tickets y documentos)
mediante colas de RabbitMQ, de modo que el flujo principal responde rápido y el
trabajo pesado se procesa por detrás. La seguridad se resuelve con un proveedor de
identidad (Keycloak): el usuario inicia sesión, obtiene un JWT y ese token se valida
tanto en el API Manager como en el backend antes de permitir cualquier operación.

## Arquitectura

```
  Navegador ──login OIDC──► Keycloak (IDaaS)
      │  obtiene JWT
      ▼
  Frontend (SPA) ──Bearer JWT──► API Manager ──valida JWT──► Backend Spring Boot
                                                                   │
                                                                   ▼
                                                   Clúster RabbitMQ (2 nodos)
                                                   3 colas + DLQ · exchanges direct/topic
```

## Qué se demuestra (elementos del sistema)

| # | Elemento | Dónde |
|---|---|---|
| 1 | Instancia de API Manager en la nube | Azure API Management |
| 2 | API Manager enruta a los endpoints del backend | `apim/validate-jwt-policy.xml` |
| 3 | Frontend consume el backend vía API Manager | `frontend/` |
| 4 | API Manager valida el JWT (acepta/rechaza) | `apim/validate-jwt-policy.xml` |
| 5 | Tenant en IDaaS + usuarios | `keycloak/realm-parkings.json` |
| 6 | Frontend con OAuth 2.0 / OpenID Connect | `frontend/app.js` |
| 7 | Backend y frontend desplegados e integrados | VM + `docker-compose.yml` |
| 8 | Clúster de 2 nodos RabbitMQ | `rabbitmq/` + servicios `rabbitmq1`/`rabbitmq2` |
| 9 | Tres colas con sus DLQ | `backend/` (notifications, tickets, documents) |
| 10 | Exchanges direct y topic | `backend/` |
| 11 | docker-compose levantando RabbitMQ en la nube | `docker-compose.yml` |

## Relación con la pauta (mensajería RabbitMQ)

Nombres centralizados en `application.yml`; beans de `Queue`/`Exchange`/`Binding`
en `config/RabbitTopologyConfig`; consumidores `@RabbitListener` por dominio con
**ACK manual** y manejo de errores (reintento / NACK / **DLQ**); microservicio
administrador REST (`admin/`) con validación de DTO; y filtro de **JWT del IDaaS**
(`security/SecurityConfig`). Incluye además mensajería **síncrona (RPC)** y
**asíncrona**, **durabilidad**, **Publish/Subscribe** (fanout), **políticas de
retención** (TTL + longitud máxima) y **alertas** en la DLQ.

## Cómo ejecutar

Requiere Docker. En la raíz del proyecto:

```bash
docker compose up -d --build
```

- Frontend: http://localhost:8081 (usuario de demo: `demo` / `demo123`)
- Panel RabbitMQ: http://localhost:15672 (nodo 2 en :15673) — guest/guest
- Keycloak (admin): http://localhost:8080 — admin/admin

Ver el clúster: `docker exec rabbitmq1 rabbitmqctl cluster_status`.

## Estructura

```
backend/    microservicio de mensajería (Spring Boot) + Dockerfile
frontend/   SPA (login OIDC + consumo vía API Manager) + nginx
keycloak/   realm-parkings.json (tenant + usuarios + cliente OIDC)
rabbitmq/   configuración del clúster de 2 nodos
apim/       política de validación de JWT para Azure API Management
docker-compose.yml   orquesta todo el stack
```
