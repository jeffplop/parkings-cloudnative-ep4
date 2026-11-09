package cl.duoc.parkings.messaging.kafka.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Datos para crear un tópico Kafka (rúbrica EP5: validación de entrada).
 * No se permiten nombres vacíos ni con caracteres inválidos. Si no se indican,
 * particiones y réplicas toman valores por defecto.
 */
public record TopicRequest(

        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Pattern(regexp = "^[a-zA-Z0-9._\\-]+$",
                 message = "El nombre solo admite letras, números y . _ -")
        String name,

        Integer partitions,
        Short replicas
) {
    public TopicRequest {
        if (partitions == null || partitions < 1) partitions = 3;
        if (replicas == null || replicas < 1) replicas = (short) 1;
    }
}
