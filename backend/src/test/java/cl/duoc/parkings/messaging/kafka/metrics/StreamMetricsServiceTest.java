package cl.duoc.parkings.messaging.kafka.metrics;

import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pruebas básicas de las métricas en tiempo real calculadas desde el flujo Kafka.
 * No requieren brokers: validan el cálculo a partir de eventos.
 */
class StreamMetricsServiceTest {

    @Test
    @DisplayName("Acumula ocupación por zona, ingresos y conteos de eventos")
    void calculaMetricas() {
        StreamMetricsService s = new StreamMetricsService();
        s.registrarOcupacion(new OcupacionEvent("E1", "CENTRO", 100, 40, "t"));
        s.registrarOcupacion(new OcupacionEvent("E2", "CENTRO", 100, 60, "t")); // misma zona: queda el último
        s.registrarPago(new PagoEvent("P1", "R1", new BigDecimal("3500"), "WEBPAY", "t"));
        s.registrarPago(new PagoEvent("P2", "R2", new BigDecimal("1500"), "WEBPAY", "t"));

        Map<String, Object> m = s.snapshot();

        assertThat(m.get("eventosOcupacionProcesados")).isEqualTo(2L);
        assertThat(m.get("eventosPagoProcesados")).isEqualTo(2L);
        assertThat((BigDecimal) m.get("ingresosAcumulados")).isEqualByComparingTo("5000");
        assertThat(m.get("ocupacionGlobalPorcentaje")).isEqualTo(60.0); // 60/100
    }
}
