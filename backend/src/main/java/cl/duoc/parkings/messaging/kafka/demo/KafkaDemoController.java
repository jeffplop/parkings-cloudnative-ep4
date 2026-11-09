package cl.duoc.parkings.messaging.kafka.demo;

import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import cl.duoc.parkings.messaging.kafka.producer.KafkaEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Controlador de DEMOSTRACIÓN para publicar eventos Kafka manualmente (p. ej.
 * desde el frontend o con curl) y ver el flujo de streaming y las métricas en
 * tiempo real. En el sistema real, estos eventos los emiten los microservicios
 * de negocio (sensores de ocupación, confirmación de pago), no un endpoint.
 */
@RestController
@RequestMapping("/api/demo/kafka")
public class KafkaDemoController {

    private final KafkaEventPublisher publisher;

    public KafkaDemoController(KafkaEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/ocupacion")
    public ResponseEntity<Map<String, String>> ocupacion(@RequestBody OcupacionEvent event) {
        publisher.publishOcupacion(event);
        return ResponseEntity.accepted().body(Map.of("status", "publicado", "topico", "ocupacion"));
    }

    @PostMapping("/pagos")
    public ResponseEntity<Map<String, String>> pago(@RequestBody PagoEvent event) {
        publisher.publishPago(event);
        return ResponseEntity.accepted().body(Map.of("status", "publicado", "topico", "pagos"));
    }
}
