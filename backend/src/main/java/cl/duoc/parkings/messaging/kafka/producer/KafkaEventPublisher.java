package cl.duoc.parkings.messaging.kafka.producer;

import cl.duoc.parkings.messaging.kafka.KafkaProperties;
import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Productor Kafka: publica eventos de negocio a los tópicos del caso. La lógica
 * de negocio llama a estos métodos sin conocer la configuración de Kafka. La
 * CLAVE del mensaje determina la partición (eventos de la misma entidad quedan
 * en la misma partición y conservan el orden).
 */
@Service
public class KafkaEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaProperties props;

    public KafkaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, KafkaProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.props = props;
    }

    /** Publica un evento de ocupación (clave = estacionamientoId). */
    public void publishOcupacion(OcupacionEvent event) {
        kafkaTemplate.send(props.getTopics().getOcupacion(), event.estacionamientoId(), event);
        log.info("[kafka→ocupacion] estacionamiento={} zona={} {}/{}",
                event.estacionamientoId(), event.zona(), event.plazasOcupadas(), event.plazasTotales());
    }

    /** Publica un evento de pago (clave = reservaId). */
    public void publishPago(PagoEvent event) {
        kafkaTemplate.send(props.getTopics().getPagos(), event.reservaId(), event);
        log.info("[kafka→pagos] pagoId={} reserva={} monto={} medio={}",
                event.pagoId(), event.reservaId(), event.monto(), event.medio());
    }
}
