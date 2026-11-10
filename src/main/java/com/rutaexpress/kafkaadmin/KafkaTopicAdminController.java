package com.rutaexpress.kafkaadmin;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/report/kafka/topics")
class KafkaTopicAdminController {
    private static final String DEMO_TOPIC = "rutaexpress\\.demo\\.[a-z0-9][a-z0-9_-]{0,59}";
    private final KafkaTopicAdminService service;

    KafkaTopicAdminController(KafkaTopicAdminService service) {
        this.service = service;
    }

    @GetMapping
    public List<String> topics() {
        return service.topics();
    }

    @GetMapping("/{topic}/retention")
    public Map<String, Long> retention(@PathVariable @Pattern(regexp = "[a-zA-Z0-9._-]{1,249}") String topic) {
        return Map.of("retentionMillis", service.retentionMillis(topic));
    }

    @GetMapping("/{topic}/partitions")
    public Map<String, Integer> partitions(@PathVariable @Pattern(regexp = "[a-zA-Z0-9._-]{1,249}") String topic) {
        return Map.of("partitions", service.partitions(topic));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void create(@RequestBody @Valid TopicRequest request) {
        service.create(request.name(), request.partitions(), request.retentionHours());
    }

    @PutMapping("/{topic}/retention")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void changeRetention(@PathVariable @Pattern(regexp = DEMO_TOPIC) String topic,
                                @RequestBody @Valid Retention request) {
        service.changeRetention(topic, request.retentionHours());
    }

    @PutMapping("/{topic}/partitions")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void increasePartitions(@PathVariable @Pattern(regexp = DEMO_TOPIC) String topic,
                                   @RequestBody @Valid Partitions request) {
        service.increasePartitions(topic, request.partitions());
    }

    @DeleteMapping("/{topic}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable @Pattern(regexp = DEMO_TOPIC) String topic) {
        service.delete(topic);
    }

    record TopicRequest(@NotBlank @Pattern(regexp = DEMO_TOPIC) String name,
                        @Min(1) @Max(12) int partitions, @Min(1) @Max(720) long retentionHours) { }

    record Retention(@Min(1) @Max(720) long retentionHours) { }
    record Partitions(@Min(1) @Max(12) int partitions) { }
}
