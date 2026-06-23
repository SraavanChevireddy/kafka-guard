package com.sraavan.kafkaguard.policy;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The governance ruleset applied to topic-provisioning requests. Every field is optional;
 * a null/empty/zero value means that constraint is not enforced.
 *
 * @param namePattern          regex the topic name must fully match (e.g. {@code (orders|payments)\.[a-z0-9_]+})
 * @param minPartitions        minimum allowed partition count (0 = unenforced)
 * @param maxPartitions        maximum allowed partition count (0 = unenforced)
 * @param minReplicationFactor minimum replication factor (0 = unenforced)
 * @param requireOwner         whether a non-blank owner is mandatory
 * @param requiredConfigs      config keys that must be present
 * @param allowedConfigValues  for a given config key, the set of values permitted
 *                             (e.g. {@code cleanup.policy -> [delete, compact]})
 * @param maxRetentionMs       upper bound on {@code retention.ms} if present (0 = unenforced)
 * @param forbiddenConfigs     config keys that must NOT be set (e.g. ban manual {@code segment.bytes})
 */
public record TopicPolicy(
        String namePattern,
        int minPartitions,
        int maxPartitions,
        int minReplicationFactor,
        boolean requireOwner,
        Set<String> requiredConfigs,
        Map<String, Set<String>> allowedConfigValues,
        long maxRetentionMs,
        Set<String> forbiddenConfigs) {

    public TopicPolicy {
        requiredConfigs = requiredConfigs == null ? Set.of() : Set.copyOf(requiredConfigs);
        forbiddenConfigs = forbiddenConfigs == null ? Set.of() : Set.copyOf(forbiddenConfigs);
        allowedConfigValues = allowedConfigValues == null ? Map.of() : Map.copyOf(allowedConfigValues);
    }

    public static TopicPolicy permissive() {
        return new TopicPolicy(null, 0, 0, 0, false, Set.of(), Map.of(), 0L, Set.of());
    }
}
