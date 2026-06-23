package com.sraavan.kafkaguard.policy;

import java.util.Map;

/**
 * A request to provision (or change) a Kafka topic, as evaluated by the governance engine.
 *
 * @param name              the topic name
 * @param partitions        requested partition count
 * @param replicationFactor requested replication factor
 * @param configs           topic-level configs (e.g. {@code retention.ms}, {@code cleanup.policy})
 * @param owner             the declared owning team/person
 */
public record TopicRequest(
        String name,
        int partitions,
        short replicationFactor,
        Map<String, String> configs,
        String owner) {

    public TopicRequest {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}
