package cl.duoc.parkings.messaging.kafka.event;

import java.math.BigDecimal;

/**
 * Evento de PAGO / transacción (streaming Kafka). Cada vez que se confirma el
 * pago de una reserva se emite este evento, que alimenta métricas de ingresos
 * en tiempo real. La clave de partición es el {@code reservaId}.
 *
 * @param pagoId    identificador del pago
 * @param reservaId reserva asociada
 * @param monto     monto pagado (CLP)
 * @param medio     medio de pago (p. ej. WEBPAY, TARJETA)
 * @param instante  marca de tiempo ISO-8601
 */
public record PagoEvent(
        String pagoId,
        String reservaId,
        BigDecimal monto,
        String medio,
        String instante
) {
}
