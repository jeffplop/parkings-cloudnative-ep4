package cl.duoc.parkings.messaging.kafka.config;

import cl.duoc.parkings.messaging.kafka.KafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declaración de los TÓPICOS de Kafka como beans (rúbrica EP5). Al arrancar,
 * {@code KafkaAdmin} los crea en el clúster si no existen, con sus particiones,
 * réplicas y parámetros de administración (retención, cleanup, min.insync).
 *
 * Son dos tópicos de negocio: ocupación y pagos, cada uno con 3 particiones y
 * 3 réplicas (requiere 3 brokers). Además se declaran sus tópicos de mensajes
 * muertos (DLT) para el manejo de errores.
 */
@Configuration
public class KafkaTopicConfig {

    private final KafkaProperties props;

    public KafkaTopicConfig(KafkaProperties props) {
        this.props = props;
    }

    @Bean
    NewTopic ocupacionTopic() {
        return trabajo(props.getTopics().getOcupacion());
    }

    @Bean
    NewTopic pagosTopic() {
        return trabajo(props.getTopics().getPagos());
    }

    @Bean
    NewTopic ocupacionDltTopic() {
        return deadLetter(props.getTopics().getOcupacion() + ".DLT");
    }

    @Bean
    NewTopic pagosDltTopic() {
        return deadLetter(props.getTopics().getPagos() + ".DLT");
    }

    /** Tópico de trabajo con particiones, réplicas y parámetros de administración. */
    private NewTopic trabajo(String name) {
        return TopicBuilder.name(name)
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(props.getRetentionMs()))
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, props.getCleanupPolicy())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    /** Tópico de mensajes muertos (mismas particiones/réplicas para conservar el orden). */
    private NewTopic deadLetter(String name) {
        return TopicBuilder.name(name)
                .partitions(props.getPartitions())
                .replicas(props.getReplicas())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(props.getRetentionMs()))
                .build();
    }
}
