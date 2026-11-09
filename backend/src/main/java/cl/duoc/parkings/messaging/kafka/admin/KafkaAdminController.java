package cl.duoc.parkings.messaging.kafka.admin;

import cl.duoc.parkings.messaging.kafka.admin.dto.TopicRequest;
import cl.duoc.parkings.messaging.kafka.metrics.StreamMetricsService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Microservicio administrador de Kafka vía REST (rúbrica EP5). Permite crear,
 * listar, describir, modificar y eliminar tópicos, inspeccionar consumer groups
 * (offsets y lag) y consultar las métricas en tiempo real del streaming.
 *
 *   POST   /api/kafka/topics                        crea un tópico
 *   GET    /api/kafka/topics                         lista los tópicos
 *   GET    /api/kafka/topics/{name}                  describe un tópico
 *   PUT    /api/kafka/topics/{name}/partitions?total=N   aumenta particiones
 *   PUT    /api/kafka/topics/{name}/retention?ms=N       cambia la retención
 *   DELETE /api/kafka/topics/{name}                  elimina un tópico
 *   GET    /api/kafka/consumer-groups                lista consumer groups
 *   GET    /api/kafka/consumer-groups/{id}           offsets y lag del grupo
 *   GET    /api/kafka/metrics                         métricas en tiempo real
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService admin;
    private final StreamMetricsService metrics;

    public KafkaAdminController(KafkaAdminService admin, StreamMetricsService metrics) {
        this.admin = admin;
        this.metrics = metrics;
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> crear(@Valid @RequestBody TopicRequest req) {
        admin.crearTopico(req.name(), req.partitions(), req.replicas());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "message", "Tópico creado", "name", req.name(),
                "partitions", req.partitions(), "replicas", req.replicas()));
    }

    @GetMapping("/topics")
    public Map<String, Object> listar() {
        return Map.of("topics", admin.listarTopicos());
    }

    @GetMapping("/topics/{name}")
    public Map<String, Object> describir(@PathVariable String name) {
        return admin.describirTopico(name);
    }

    @PutMapping("/topics/{name}/partitions")
    public Map<String, Object> aumentarParticiones(@PathVariable String name, @RequestParam int total) {
        admin.aumentarParticiones(name, total);
        return Map.of("message", "Particiones actualizadas", "name", name, "total", total);
    }

    @PutMapping("/topics/{name}/retention")
    public Map<String, Object> retencion(@PathVariable String name, @RequestParam long ms) {
        admin.actualizarRetencion(name, ms);
        return Map.of("message", "Retención actualizada", "name", name, "retentionMs", ms);
    }

    @DeleteMapping("/topics/{name}")
    public Map<String, Object> eliminar(@PathVariable String name) {
        admin.eliminarTopico(name);
        return Map.of("message", "Tópico eliminado", "name", name);
    }

    @GetMapping("/consumer-groups")
    public Map<String, Object> grupos() {
        return Map.of("consumerGroups", admin.listarConsumerGroups());
    }

    @GetMapping("/consumer-groups/{id}")
    public Map<String, Object> grupo(@PathVariable String id) {
        return admin.describirConsumerGroup(id);
    }

    @GetMapping("/metrics")
    public Map<String, Object> metricas() {
        return metrics.snapshot();
    }

    /** 400 con los mensajes de validación claros. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> validacion(MethodArgumentNotValidException ex) {
        Map<String, String> errores = new HashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(fe -> errores.put(fe.getField(), fe.getDefaultMessage()));
        return Map.of("error", "Parámetros inválidos", "detalles", errores);
    }

    /** 502 cuando falla una operación de administración contra el clúster. */
    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, Object> adminError(IllegalStateException ex) {
        return Map.of("error", "Error de administración de Kafka", "detalle", ex.getMessage());
    }
}
