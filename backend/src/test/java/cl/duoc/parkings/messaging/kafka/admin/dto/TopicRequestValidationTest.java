package cl.duoc.parkings.messaging.kafka.admin.dto;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pruebas básicas de la validación del DTO para crear tópicos Kafka (rúbrica EP5).
 */
class TopicRequestValidationTest {

    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void setUp() {
        factory = Validation.byDefaultProvider().configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void tearDown() {
        if (factory != null) factory.close();
    }

    @Test
    @DisplayName("Tópico válido no genera violaciones")
    void valido() {
        assertThat(validator.validate(new TopicRequest("parkings.ocupacion", 3, (short) 3))).isEmpty();
    }

    @Test
    @DisplayName("Nombre vacío es rechazado")
    void nombreVacio() {
        assertThat(validator.validate(new TopicRequest("", 3, (short) 3))).isNotEmpty();
    }

    @Test
    @DisplayName("Nombre con caracteres inválidos es rechazado")
    void nombreInvalido() {
        assertThat(validator.validate(new TopicRequest("mal nombre!", 3, (short) 3))).isNotEmpty();
    }

    @Test
    @DisplayName("Sin particiones/réplicas, toma valores por defecto (3 y 1)")
    void defaults() {
        TopicRequest r = new TopicRequest("parkings.demo", null, null);
        assertThat(r.partitions()).isEqualTo(3);
        assertThat(r.replicas()).isEqualTo((short) 1);
    }
}
