package cl.duoc.parkings.messaging.kafka.metrics;

import cl.duoc.parkings.messaging.kafka.event.OcupacionEvent;
import cl.duoc.parkings.messaging.kafka.event.PagoEvent;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MÉTRICAS EN TIEMPO REAL calculadas a partir del flujo de eventos Kafka.
 * Los consumidores alimentan este servicio a medida que llegan eventos, y el
 * resultado se expone por REST (GET /api/kafka/metrics). Demuestra cómo el
 * streaming permite "reaccionar" a los eventos y actualizar estados al vuelo.
 * Además guarda los últimos eventos con su partición y offset de Kafka.
 */
@Service
public class StreamMetricsService {

    private static final int MAX_RECIENTES = 12;

    /** zona -> [plazasOcupadas, plazasTotales] del último evento por zona. */
    private final Map<String, int[]> ocupacionPorZona = new ConcurrentHashMap<>();
    private final AtomicLong eventosOcupacion = new AtomicLong();
    private final AtomicLong eventosPago = new AtomicLong();
    private final AtomicReference<BigDecimal> ingresosAcumulados = new AtomicReference<>(BigDecimal.ZERO);
    /** Últimos eventos recibidos (más reciente primero), con partición y offset. */
    private final ConcurrentLinkedDeque<Map<String, Object>> recientes = new ConcurrentLinkedDeque<>();

    public void registrarOcupacion(OcupacionEvent e) {
        ocupacionPorZona.put(e.zona(), new int[]{ e.plazasOcupadas(), e.plazasTotales() });
        eventosOcupacion.incrementAndGet();
    }

    public void registrarPago(PagoEvent e) {
        ingresosAcumulados.updateAndGet(acc -> acc.add(e.monto()));
        eventosPago.incrementAndGet();
    }

    /** Registra un evento en la lista de "últimos eventos" con su partición y offset. */
    public void registrarEvento(String tipo, String detalle, int particion, long offset) {
        Map<String, Object> ev = new LinkedHashMap<>();
        ev.put("tipo", tipo);
        ev.put("detalle", detalle);
        ev.put("particion", particion);
        ev.put("offset", offset);
        recientes.addFirst(ev);
        while (recientes.size() > MAX_RECIENTES) {
            recientes.pollLast();
        }
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
        m.put("ultimosEventos", new ArrayList<>(recientes));
        return m;
    }
}
