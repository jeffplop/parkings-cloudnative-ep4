package cl.duoc.parkings.messaging.kafka.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Manejo de ERRORES en el consumo de eventos Kafka (rúbrica EP5).
 *
 * Si un consumidor lanza una excepción al procesar un evento, este manejador
 * REINTENTA 2 veces (backoff de 1 s). Si sigue fallando, el evento se publica
 * en el tópico de mensajes muertos {@code <topico>.DLT}, de modo que un mensaje
 * defectuoso no detiene el procesamiento del resto del flujo.
 *
 * Spring Boot inyecta automáticamente este {@code DefaultErrorHandler} en la
 * fábrica de listeners autoconfigurada.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        // 2 reintentos separados por 1 segundo; luego, a la DLT.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}
