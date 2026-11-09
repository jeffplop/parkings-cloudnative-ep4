package cl.duoc.parkings.messaging.kafka.event;

/**
 * Evento de telemetría de OCUPACIÓN de un estacionamiento (streaming Kafka).
 * Se emite de forma continua para alimentar métricas de disponibilidad en
 * tiempo real. La clave de partición es el {@code estacionamientoId}, de modo
 * que los eventos de un mismo estacionamiento mantienen el orden.
 *
 * @param estacionamientoId identificador del estacionamiento
 * @param zona              zona/comuna (p. ej. CENTRO, PROVIDENCIA)
 * @param plazasTotales     total de plazas del estacionamiento
 * @param plazasOcupadas    plazas ocupadas en este instante
 * @param instante          marca de tiempo ISO-8601 de la medición
 */
public record OcupacionEvent(
        String estacionamientoId,
        String zona,
        int plazasTotales,
        int plazasOcupadas,
        String instante
) {
}
