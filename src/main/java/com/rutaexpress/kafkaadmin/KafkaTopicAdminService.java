package com.rutaexpress.kafkaadmin;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
class KafkaTopicAdminService {
    private final Admin admin;
    private final short replicas;

    KafkaTopicAdminService(Admin admin, @Value("${kafka.admin.replicas}") short replicas) {
        this.admin = admin;
        this.replicas = replicas;
    }

    List<String> topics() {
        return await(admin.listTopics().names()).stream().sorted().toList();
    }

    long retentionMillis(String topic) {
        var resource = topicResource(topic);
        var config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        return Long.parseLong(config.get("retention.ms").value());
    }

    int partitions(String topic) {
        return await(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic).partitions().size();
    }

    void create(String topic, int partitions, long retentionHours) {
        var definition = new NewTopic(topic, partitions, replicas)
                .configs(Map.of("retention.ms", retentionValue(retentionHours)));
        await(admin.createTopics(List.of(definition)).all());
    }

    void changeRetention(String topic, long retentionHours) {
        var retention = new ConfigEntry("retention.ms", retentionValue(retentionHours));
        var operation = new AlterConfigOp(retention, AlterConfigOp.OpType.SET);
        await(admin.incrementalAlterConfigs(Map.of(topicResource(topic), List.of(operation))).all());
    }

    void increasePartitions(String topic, int total) {
        int existing = partitions(topic);
        if (total == existing) return;
        if (total < existing) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Kafka solo permite aumentar las particiones");
        }
        await(admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
    }

    void delete(String topic) {
        await(admin.deleteTopics(List.of(topic)).all());
    }

    private static ConfigResource topicResource(String name) {
        return new ConfigResource(ConfigResource.Type.TOPIC, name);
    }

    private static String retentionValue(long hours) {
        return Long.toString(Duration.ofHours(hours).toMillis());
    }

    private static <T> T await(KafkaFuture<T> result) {
        try {
            return result.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Operación de Kafka interrumpida");
        } catch (TimeoutException ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no respondió a tiempo");
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof TopicExistsException) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Ya existe ese tópico");
            }
            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No existe ese tópico");
            }
            if (cause instanceof InvalidConfigurationException || cause instanceof InvalidReplicationFactorException
                    || cause instanceof InvalidPartitionsException) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Configuración de tópico inválida");
            }
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no pudo completar la operación");
        }
    }
}
