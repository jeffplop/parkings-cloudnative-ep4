package cl.duoc.parkings.messaging;

import cl.duoc.parkings.messaging.config.RabbitProperties;
import cl.duoc.parkings.messaging.kafka.KafkaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Microservicio de mensajería de Parkings Together (DSY1107 — EP3/EP5).
 *
 * Integra RabbitMQ para tareas asíncronas (notificaciones, tickets, documentos)
 * y Apache Kafka para streaming de eventos de negocio (ocupación y pagos) con
 * métricas en tiempo real. Incluye los microservicios administradores de ambas
 * plataformas (topología RabbitMQ y tópicos Kafka) vía REST.
 */
@SpringBootApplication
@EnableConfigurationProperties({ RabbitProperties.class, KafkaProperties.class })
@EnableScheduling
public class MessagingApplication {

    public static void main(String[] args) {
        SpringApplication.run(MessagingApplication.class, args);
    }
}
