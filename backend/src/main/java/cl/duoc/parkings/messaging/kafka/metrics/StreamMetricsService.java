package cl.duoc.parkings.messaging.kafka.metrics;

import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MÉTRICAS EN TIEMPO REAL calculadas a partir del flujo de eventos Kafka.
 * Los consumidores alimentan este servicio a medida que llegan eventos, y el
 * resultado se expone por REST (GET /api/kafka/metrics). Demuestra cómo el
 * streaming permite "reaccionar" a los eventos y actualizar estados al vuelo.
 */
@Service
public class StreamMetricsService {

    /** zona -> [plazasOcupadas, plazasTotales] del último evento por zona. */
    private final Map<String, int[]> ocupacionPorZona = new ConcurrentHashMap<>();
    private final AtomicLong eventosOcupacion = new AtomicLong();
    private final AtomicLong eventosPago = new AtomicLong();
    private final AtomicReference<BigDecimal> ingresosAcumulados = new AtomicReference<>(BigDecimal.ZERO);

    public void registrarOcupacion(OcupacionEvent e) {
        ocupacionPorZona.put(e.zona(), new int[]{ e.plazasOcupadas(), e.plazasTotales() });
        eventosOcupacion.incrementAndGet();
    }

    public void registrarPago(PagoEvent e) {
        ingresosAcumulados.updateAndGet(acc -> acc.add(e.monto()));
        eventosPago.incrementAndGet();
    }

    /** Fotografía de las métricas actuales (lo que verá el frontend/monitor). */
    public Map<String, Object> snapshot() {
        long ocupadas = 0, totales = 0;
        Map<String, Object> porZona = new LinkedHashMap<>();
        for (var entry : ocupacionPorZona.entrySet()) {
            int[] v = entry.getValue();
            ocupadas += v[0];
            totales += v[1];
            porZona.put(entry.getKey(), Map.of(
                    "ocupadas", v[0], "totales", v[1],
                    "porcentaje", v[1] == 0 ? 0 : Math.round(v[0] * 1000.0 / v[1]) / 10.0));
        }
        double ocupacionGlobal = totales == 0 ? 0 : Math.round(ocupadas * 1000.0 / totales) / 10.0;

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ocupacionGlobalPorcentaje", ocupacionGlobal);
        m.put("plazasOcupadas", ocupadas);
        m.put("plazasTotales", totales);
        m.put("ocupacionPorZona", porZona);
        m.put("ingresosAcumulados", ingresosAcumulados.get());
        m.put("eventosOcupacionProcesados", eventosOcupacion.get());
        m.put("eventosPagoProcesados", eventosPago.get());
        return m;
    }
}
