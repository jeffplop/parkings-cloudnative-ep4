package cl.duoc.parkings.messaging.kafka.consumer;

import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import cl.duoc.parkings.messaging.kafka.metrics.StreamMetricsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Consumidor Kafka del tópico de PAGOS (rúbrica EP5). Consumer group propio y
 * concurrency=3 (un hilo por partición). Lee partición y offset del
 * {@code ConsumerRecord} y actualiza la métrica de ingresos en tiempo real.
 * La lógica de dominio está separada de la mensajería; los errores van a
 * reintento/DLT.
 */
@Component
public class PagoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PagoConsumer.class);

    private final StreamMetricsService metrics;

    public PagoConsumer(StreamMetricsService metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = "${parkings.kafka.topics.pagos}",
            groupId = "${parkings.kafka.groups.pagos}",
            concurrency = "3")
    public void onPago(ConsumerRecord<String, PagoEvent> record) {
        PagoEvent event = record.value();
        validar(event);
        metrics.registrarPago(event);
        metrics.registrarEvento("pago",
                event.reservaId() + " · $" + event.monto() + " · " + event.medio(),
                record.partition(), record.offset());
        log.info("[kafka:pagos] p{} off{} pagoId={} reserva={} monto={} medio={}",
                record.partition(), record.offset(), event.pagoId(), event.reservaId(), event.monto(), event.medio());
    }

    private void validar(PagoEvent e) {
        if (e.reservaId() == null || e.reservaId().isBlank()) {
            throw new IllegalArgumentException("Pago sin reservaId");
        }
        if (e.monto() == null || e.monto().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Monto de pago inválido: " + e.monto());
        }
    }
}
