package cl.duoc.parkings.messaging.kafka.consumer;

import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.metrics.StreamMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka del tópico de OCUPACIÓN (rúbrica EP5). Usa {@code @KafkaListener}
 * y pertenece a un consumer group propio; con concurrency=3 hay un hilo por
 * partición (balanceo dentro del grupo). La lógica de dominio (validar + actualizar
 * métricas) está separada de la infraestructura de mensajería.
 *
 * Si la validación falla, lanza una excepción: el manejador de errores reintenta
 * y, si persiste, envía el evento a la DLT (ver KafkaConsumerConfig).
 */
@Component
public class OcupacionConsumer {

    private static final Logger log = LoggerFactory.getLogger(OcupacionConsumer.class);

    private final StreamMetricsService metrics;

    public OcupacionConsumer(StreamMetricsService metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = "${parkings.kafka.topics.ocupacion}",
            groupId = "${parkings.kafka.groups.ocupacion}",
            concurrency = "3")
    public void onOcupacion(OcupacionEvent event) {
        validar(event);
        metrics.registrarOcupacion(event);
        log.info("[kafka:ocupacion] estacionamiento={} zona={} ocupadas={}/{}",
                event.estacionamientoId(), event.zona(), event.plazasOcupadas(), event.plazasTotales());
    }

    /** Validación del payload: un evento inconsistente se rechaza (irá a reintento/DLT). */
    private void validar(OcupacionEvent e) {
        if (e.estacionamientoId() == null || e.estacionamientoId().isBlank()) {
            throw new IllegalArgumentException("Evento de ocupación sin estacionamientoId");
        }
        if (e.plazasTotales() <= 0 || e.plazasOcupadas() < 0 || e.plazasOcupadas() > e.plazasTotales()) {
            throw new IllegalArgumentException("Plazas inconsistentes: " + e.plazasOcupadas() + "/" + e.plazasTotales());
        }
    }
}
