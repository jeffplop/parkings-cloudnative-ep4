# Parkings Together — Cloud Native (DSY1107)

Proyecto final de **Desarrollo Cloud Native I**. Integra, sobre el caso *Parkings
Together*, una arquitectura cloud-native con:

- **Mensajería MQ** con **RabbitMQ** (clúster de 2 nodos, colas, exchanges, DLQ).
- **Streaming** con **Apache Kafka** (clúster de 3 brokers + Zookeeper, 2 tópicos,
  productores/consumidores y métricas en tiempo real).
- **Seguridad** con **IDaaS** (Keycloak, OAuth 2.0 / OpenID Connect) y validación
  de **JWT** en el **API Manager** y en el backend.
- **Frontend** SPA que inicia sesión, consume el backend y visualiza las métricas.
- Todo orquestado con **docker-compose**.

**Integrantes:** Jefferson Rojas · Gabriel Molina · Guillermo Santander — **Sección:** 002V

---

## Resumen ejecutivo

El sistema desacopla el trabajo del flujo principal con dos mecanismos
complementarios: **RabbitMQ** para tareas asíncronas dirigidas (notificaciones,
tickets, documentos) y **Kafka** para el *streaming* de eventos de negocio
(ocupación de estacionamientos y pagos), que alimentan **métricas en tiempo real**.
La seguridad se resuelve con Keycloak: el usuario inicia sesión, obtiene un JWT y
ese token se valida en el API Manager y en el backend antes de operar.

## Arquitectura

```
  Navegador ──login OIDC──► Keycloak (IDaaS)
      │ JWT
      ▼
  Frontend (SPA) ─Bearer JWT─► API Manager ─valida JWT─► Backend Spring Boot
                                                              │
                        ┌─────────────────────────────────────┼───────────────────────────────┐
                        ▼                                     ▼                                 ▼
             Clúster RabbitMQ (2 nodos)            Clúster Kafka (3 brokers + 3 ZK)      Métricas en
             3 colas + DLQ · direct/topic          2 tópicos × 3 particiones × 3 réplicas  tiempo real
                                                    productores/consumidores · Kafka UI
```

## Qué se demuestra

**RabbitMQ / IDaaS / API Manager**

| Elemento | Dónde |
|---|---|
| API Manager valida JWT (acepta/rechaza) | `apim/validate-jwt-policy.xml` |
| Frontend OAuth 2.0 / OIDC + consumo vía API Manager | `frontend/` |
| Tenant en IDaaS + usuarios | `keycloak/realm-parkings.json` |
| Clúster de 2 nodos RabbitMQ · 3 colas + DLQ · exchanges direct/topic | `rabbitmq/` + `backend/` |
| docker-compose levantando la arquitectura | `docker-compose.yml` |

**Apache Kafka (EP5/EP6)**

| Elemento | Dónde |
|---|---|
| Cliente Kafka (brokers, serializers, consumer groups) | `application.yml` (`spring.kafka.*`) |
| 2 tópicos con 3 particiones y 3 réplicas + retención | `kafka/config/KafkaTopicConfig` |
| Productores con DTO tipados | `kafka/producer/KafkaEventPublisher` + `kafka/event/*` |
| Consumidores `@KafkaListener` por dominio y consumer group | `kafka/consumer/*` |
| Manejo de errores (validación + reintentos + **DLT**) | `kafka/config/KafkaConsumerConfig` |
| Microservicio administrador de Kafka (tópicos + consumer groups) | `kafka/admin/*` |
| Métricas en tiempo real (ocupación, ingresos) | `kafka/metrics/StreamMetricsService` |
| Flujo continuo de eventos (streaming en vivo) | `kafka/generator/StreamGenerator` |
| Kafka UI (administración de brokers/tópicos/mensajes) | servicio `kafka-ui` |

## Cómo ejecutar

Requiere Docker Desktop. En la raíz del proyecto:

```bash
docker compose up -d --build
```

| Servicio | URL |
|---|---|
| Frontend (SPA) | http://localhost:8081 — usuario `demo` / `demo123` |
| Kafka UI | http://localhost:8082 |
| Panel RabbitMQ | http://localhost:15672 (nodo 2 en :15673) — guest/guest |
| Keycloak (admin) | http://localhost:8080 — admin/admin |

Comprobaciones rápidas:
```bash
docker exec rabbitmq1 rabbitmqctl cluster_status
docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic parkings.ocupacion
```

El backend emite un **flujo continuo** de eventos (ocupación y pagos). En el
frontend, tras iniciar sesión, pulsa **“Métricas en vivo”** para ver cómo se
actualizan en tiempo real, y abre **Kafka UI** para ver los tópicos, particiones,
mensajes, offsets y lag.

## Estructura

```
backend/    microservicio Spring Boot (RabbitMQ + Kafka) + Dockerfile
  └ kafka/  config · event (DTO) · producer · consumer · admin · metrics · generator
frontend/   SPA (login OIDC + consumo + métricas) + nginx
keycloak/   realm-parkings.json (tenant + usuarios + cliente OIDC)
rabbitmq/   configuración del clúster de 2 nodos
apim/       política de validación de JWT para el API Manager
docker-compose.yml   orquesta RabbitMQ, Kafka (ZK + brokers + UI), Keycloak, backend y frontend
```
