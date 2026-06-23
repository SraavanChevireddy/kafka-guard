package com.sraavan.kafkaguard.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates a {@link TopicRequest} against a {@link TopicPolicy}. Pure and dependency-free
 * so the governance rules can be unit-tested exhaustively without a Kafka cluster.
 *
 * <p>Collects every violation rather than stopping at the first, so a caller (or CI job)
 * sees the complete set of problems with a proposed topic.
 */
public final class TopicPolicyValidator {

    public ValidationResult validate(TopicPolicy policy, TopicRequest req) {
        List<ValidationResult.Violation> v = new ArrayList<>();

        validateName(policy, req, v);
        validatePartitions(policy, req, v);
        validateReplication(policy, req, v);
        validateOwner(policy, req, v);
        validateRequiredConfigs(policy, req, v);
        validateForbiddenConfigs(policy, req, v);
        validateAllowedValues(policy, req, v);
        validateRetention(policy, req, v);

        return v.isEmpty() ? ValidationResult.ok() : ValidationResult.failed(v);
    }

    private void validateName(TopicPolicy p, TopicRequest req, List<ValidationResult.Violation> v) {
        if (p.namePattern() == null || p.namePattern().isBlank()) {
            return;
        }
        try {
            if (!Pattern.matches(p.namePattern(), req.name())) {
                v.add(new ValidationResult.Violation("NAME_PATTERN",
                        "Topic name '" + req.name() + "' does not match required pattern '"
                                + p.namePattern() + "'"));
            }
        } catch (PatternSyntaxException e) {
            v.add(new ValidationResult.Violation("BAD_NAME_PATTERN",
                    "Policy name pattern is not a valid regex: " + e.getMessage()));
        }
    }

    private void validatePartitions(TopicPolicy p, TopicRequest req, List<ValidationResult.Violation> v) {
        if (req.partitions() <= 0) {
            v.add(new ValidationResult.Violation("PARTITIONS_INVALID",
                    "Partition count must be positive"));
            return;
        }
        if (p.minPartitions() > 0 && req.partitions() < p.minPartitions()) {
            v.add(new ValidationResult.Violation("PARTITIONS_TOO_FEW",
                    "Partitions " + req.partitions() + " is below the minimum " + p.minPartitions()));
        }
        if (p.maxPartitions() > 0 && req.partitions() > p.maxPartitions()) {
            v.add(new ValidationResult.Violation("PARTITIONS_TOO_MANY",
                    "Partitions " + req.partitions() + " exceeds the maximum " + p.maxPartitions()));
        }
    }

    private void validateReplication(TopicPolicy p, TopicRequest req, List<ValidationResult.Violation> v) {
        if (p.minReplicationFactor() > 0 && req.replicationFactor() < p.minReplicationFactor()) {
            v.add(new ValidationResult.Violation("REPLICATION_TOO_LOW",
                    "Replication factor " + req.replicationFactor() + " is below the minimum "
                            + p.minReplicationFactor()));
        }
    }

    private void validateOwner(TopicPolicy p, TopicRequest req, List<ValidationResult.Violation> v) {
        if (p.requireOwner() && (req.owner() == null || req.owner().isBlank())) {
            v.add(new ValidationResult.Violation("OWNER_REQUIRED",
                    "An owner must be declared for every topic"));
        }
    }

    private void validateRequiredConfigs(TopicPolicy p, TopicRequest req,
                                         List<ValidationResult.Violation> v) {
        for (String key : p.requiredConfigs()) {
            if (!req.configs().containsKey(key)) {
                v.add(new ValidationResult.Violation("MISSING_CONFIG",
                        "Required config '" + key + "' is not set"));
            }
        }
    }

    private void validateForbiddenConfigs(TopicPolicy p, TopicRequest req,
                                          List<ValidationResult.Violation> v) {
        for (String key : p.forbiddenConfigs()) {
            if (req.configs().containsKey(key)) {
                v.add(new ValidationResult.Violation("FORBIDDEN_CONFIG",
                        "Config '" + key + "' may not be set by requesters"));
            }
        }
    }

    private void validateAllowedValues(TopicPolicy p, TopicRequest req,
                                       List<ValidationResult.Violation> v) {
        for (Map.Entry<String, Set<String>> e : p.allowedConfigValues().entrySet()) {
            String key = e.getKey();
            Set<String> allowed = e.getValue();
            String actual = req.configs().get(key);
            if (actual != null && !allowed.contains(actual)) {
                v.add(new ValidationResult.Violation("CONFIG_VALUE_NOT_ALLOWED",
                        "Config '" + key + "'='" + actual + "' is not one of the allowed values "
                                + allowed));
            }
        }
    }

    private void validateRetention(TopicPolicy p, TopicRequest req, List<ValidationResult.Violation> v) {
        if (p.maxRetentionMs() <= 0) {
            return;
        }
        String retention = req.configs().get("retention.ms");
        if (retention == null) {
            return;
        }
        try {
            long ms = Long.parseLong(retention.trim());
            // retention.ms = -1 means "infinite" in Kafka; reject it when a cap is enforced.
            if (ms < 0 || ms > p.maxRetentionMs()) {
                v.add(new ValidationResult.Violation("RETENTION_TOO_LONG",
                        "retention.ms=" + retention + " exceeds the maximum " + p.maxRetentionMs()
                                + (ms < 0 ? " (infinite retention not allowed)" : "")));
            }
        } catch (NumberFormatException e) {
            v.add(new ValidationResult.Violation("RETENTION_NOT_NUMERIC",
                    "retention.ms='" + retention + "' is not a number"));
        }
    }
}
