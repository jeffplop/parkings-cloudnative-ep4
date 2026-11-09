package cl.duoc.parkings.messaging.kafka.generator;

import cl.duoc.parkings.messaging.kafka.KafkaProperties;
import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import cl.duoc.parkings.messaging.kafka.producer.KafkaEventPublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;

/**
 * Generador de FLUJO CONTINUO de eventos para demostrar el streaming en vivo
 * (EP5/EP6: "visualizar el flujo continuo de datos"). Cuando está habilitado
 * (parkings.kafka.stream-generator.enabled=true), emite periódicamente eventos
 * de ocupación y, de vez en cuando, de pago, para que se vean fluir en Kafka UI
 * y las métricas en tiempo real se actualicen solas.
 */
@Component
public class StreamGenerator {

    private final KafkaEventPublisher publisher;
    private final KafkaProperties props;
    private final Random rnd = new Random();
    private static final String[] ZONAS = {"CENTRO", "PROVIDENCIA", "LAS CONDES", "NUNOA", "MAIPU"};

    public StreamGenerator(KafkaEventPublisher publisher, KafkaProperties props) {
        this.publisher = publisher;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${parkings.kafka.stream-generator.interval-ms:3000}")
    public void emitir() {
        if (!props.getStreamGenerator().isEnabled()) {
            return;
        }
        String zona = ZONAS[rnd.nextInt(ZONAS.length)];
        String estacionamiento = "EST-" + (100 + rnd.nextInt(20));
        int totales = 50 + rnd.nextInt(150);
        int ocupadas = rnd.nextInt(totales + 1);
        publisher.publishOcupacion(new OcupacionEvent(
                estacionamiento, zona, totales, ocupadas, Instant.now().toString()));

        // Aproximadamente 1 de cada 3 ciclos también emite un pago.
        if (rnd.nextInt(3) == 0) {
            publisher.publishPago(new PagoEvent(
                    "PAY-" + System.currentTimeMillis(),
                    "R-" + (1000 + rnd.nextInt(9000)),
                    BigDecimal.valueOf(1000L * (1 + rnd.nextInt(10))),
                    "WEBPAY",
                    Instant.now().toString()));
        }
    }
}
