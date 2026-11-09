package cl.duoc.parkings.messaging.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuración CENTRALIZADA de Apache Kafka (EP5). Enlaza la sección
 * {@code parkings.kafka.*} de application.yml: nombres de tópicos, número de
 * particiones y réplicas, retención, política de limpieza y consumer groups.
 * Ningún nombre de tópico o grupo se escribe suelto en el código.
 */
@ConfigurationProperties(prefix = "parkings.kafka")
public class KafkaProperties {

    /** Número de particiones por tópico (paralelismo). */
    private int partitions = 3;
    /** Factor de replicación por tópico (tolerancia a fallos; requiere N brokers). */
    private short replicas = 3;
    /** Retención de eventos en milisegundos. */
    private long retentionMs = 604800000L;
    /** Política de limpieza del log (delete | compact). */
    private String cleanupPolicy = "delete";

    private Topics topics = new Topics();
    private Groups groups = new Groups();
    private StreamGenerator streamGenerator = new StreamGenerator();

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public short getReplicas() { return replicas; }
    public void setReplicas(short replicas) { this.replicas = replicas; }

    public long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(long retentionMs) { this.retentionMs = retentionMs; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }

    public Topics getTopics() { return topics; }
    public void setTopics(Topics topics) { this.topics = topics; }

    public Groups getGroups() { return groups; }
    public void setGroups(Groups groups) { this.groups = groups; }

    public StreamGenerator getStreamGenerator() { return streamGenerator; }
    public void setStreamGenerator(StreamGenerator streamGenerator) { this.streamGenerator = streamGenerator; }

    /** Nombres de los dos tópicos del caso Parkings Together. */
    public static class Topics {
        private String ocupacion;   // telemetría de ocupación de estacionamientos
        private String pagos;       // eventos de pago / transacciones
        public String getOcupacion() { return ocupacion; }
        public void setOcupacion(String ocupacion) { this.ocupacion = ocupacion; }
        public String getPagos() { return pagos; }
        public void setPagos(String pagos) { this.pagos = pagos; }
    }

    /** Consumer groups (balanceo de carga / procesamiento resiliente). */
    public static class Groups {
        private String ocupacion;
        private String pagos;
        public String getOcupacion() { return ocupacion; }
        public void setOcupacion(String ocupacion) { this.ocupacion = ocupacion; }
        public String getPagos() { return pagos; }
        public void setPagos(String pagos) { this.pagos = pagos; }
    }

    /** Generador de flujo CONTINUO para demostrar el streaming en vivo. */
    public static class StreamGenerator {
        private boolean enabled = false;
        private long intervalMs = 3000;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
    }
}
