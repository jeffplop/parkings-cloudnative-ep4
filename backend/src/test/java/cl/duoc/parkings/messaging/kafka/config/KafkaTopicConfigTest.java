package cl.duoc.parkings.messaging.kafka.config;

import cl.duoc.parkings.messaging.kafka.KafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pruebas básicas de la declaración de tópicos Kafka (rúbrica EP5): verifican
 * particiones, réplicas y parámetros de administración (retención, cleanup).
 */
class KafkaTopicConfigTest {

    private KafkaTopicConfig config;

    @BeforeEach
    void setUp() {
        KafkaProperties p = new KafkaProperties();
        p.setPartitions(3);
        p.setReplicas((short) 3);
        p.setRetentionMs(604800000L);
        p.setCleanupPolicy("delete");
        KafkaProperties.Topics t = new KafkaProperties.Topics();
        t.setOcupacion("parkings.ocupacion");
        t.setPagos("parkings.pagos");
        p.setTopics(t);
        config = new KafkaTopicConfig(p);
    }

    @Test
    @DisplayName("Los tópicos se declaran con 3 particiones, 3 réplicas y retención")
    void topicos() {
        NewTopic ocupacion = config.ocupacionTopic();
        assertThat(ocupacion.name()).isEqualTo("parkings.ocupacion");
        assertThat(ocupacion.numPartitions()).isEqualTo(3);
        assertThat(ocupacion.replicationFactor()).isEqualTo((short) 3);
        assertThat(ocupacion.configs()).containsEntry(TopicConfig.RETENTION_MS_CONFIG, "604800000");
        assertThat(ocupacion.configs()).containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");

        NewTopic pagos = config.pagosTopic();
        assertThat(pagos.name()).isEqualTo("parkings.pagos");
        assertThat(pagos.numPartitions()).isEqualTo(3);
    }
}
