package com.sraavan.kafkaguard.policy;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicPolicyValidatorTest {

    private final TopicPolicyValidator validator = new TopicPolicyValidator();

    private TopicRequest req(String name, int partitions, short rf, Map<String, String> cfg, String owner) {
        return new TopicRequest(name, partitions, rf, cfg, owner);
    }

    private boolean hasRule(ValidationResult r, String rule) {
        return r.violations().stream().anyMatch(v -> v.rule().equals(rule));
    }

    @Test
    void permissivePolicyAcceptsAnyValidTopic() {
        ValidationResult r = validator.validate(TopicPolicy.permissive(),
                req("whatever", 1, (short) 1, Map.of(), null));
        assertTrue(r.valid());
    }

    @Test
    void nonPositivePartitionsAlwaysInvalid() {
        ValidationResult r = validator.validate(TopicPolicy.permissive(),
                req("t", 0, (short) 1, Map.of(), null));
        assertFalse(r.valid());
        assertTrue(hasRule(r, "PARTITIONS_INVALID"));
    }

    @Test
    void nameMustMatchPattern() {
        TopicPolicy p = new TopicPolicy("(orders|payments)\\.[a-z0-9_]+",
                0, 0, 0, false, Set.of(), Map.of(), 0L, Set.of());
        assertTrue(validator.validate(p, req("orders.created", 1, (short) 1, Map.of(), null)).valid());
        ValidationResult bad = validator.validate(p, req("RandomTopic", 1, (short) 1, Map.of(), null));
        assertFalse(bad.valid());
        assertTrue(hasRule(bad, "NAME_PATTERN"));
    }

    @Test
    void invalidRegexInPolicyIsReported() {
        TopicPolicy p = new TopicPolicy("([unclosed",
                0, 0, 0, false, Set.of(), Map.of(), 0L, Set.of());
        ValidationResult r = validator.validate(p, req("anything", 1, (short) 1, Map.of(), null));
        assertFalse(r.valid());
        assertTrue(hasRule(r, "BAD_NAME_PATTERN"));
    }

    @Test
    void partitionBoundsEnforced() {
        TopicPolicy p = new TopicPolicy(null, 3, 12, 0, false, Set.of(), Map.of(), 0L, Set.of());
        assertTrue(hasRule(validator.validate(p, req("t", 1, (short) 1, Map.of(), null)),
                "PARTITIONS_TOO_FEW"));
        assertTrue(hasRule(validator.validate(p, req("t", 50, (short) 1, Map.of(), null)),
                "PARTITIONS_TOO_MANY"));
        assertTrue(validator.validate(p, req("t", 6, (short) 1, Map.of(), null)).valid());
    }

    @Test
    void minReplicationEnforced() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 3, false, Set.of(), Map.of(), 0L, Set.of());
        assertTrue(hasRule(validator.validate(p, req("t", 1, (short) 2, Map.of(), null)),
                "REPLICATION_TOO_LOW"));
        assertTrue(validator.validate(p, req("t", 1, (short) 3, Map.of(), null)).valid());
    }

    @Test
    void ownerRequiredWhenConfigured() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 0, true, Set.of(), Map.of(), 0L, Set.of());
        assertTrue(hasRule(validator.validate(p, req("t", 1, (short) 1, Map.of(), "  ")),
                "OWNER_REQUIRED"));
        assertTrue(validator.validate(p, req("t", 1, (short) 1, Map.of(), "team-orders")).valid());
    }

    @Test
    void requiredAndForbiddenConfigsEnforced() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 0, false,
                Set.of("retention.ms"), Map.of(), 0L, Set.of("segment.bytes"));
        ValidationResult missing = validator.validate(p,
                req("t", 1, (short) 1, Map.of("segment.bytes", "1024"), null));
        assertFalse(missing.valid());
        assertTrue(hasRule(missing, "MISSING_CONFIG"));
        assertTrue(hasRule(missing, "FORBIDDEN_CONFIG"));
    }

    @Test
    void allowedConfigValuesEnforced() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 0, false, Set.of(),
                Map.of("cleanup.policy", Set.of("delete", "compact")), 0L, Set.of());
        assertTrue(hasRule(validator.validate(p,
                req("t", 1, (short) 1, Map.of("cleanup.policy", "evict"), null)),
                "CONFIG_VALUE_NOT_ALLOWED"));
        assertTrue(validator.validate(p,
                req("t", 1, (short) 1, Map.of("cleanup.policy", "compact"), null)).valid());
    }

    @Test
    void retentionCapEnforcedAndInfiniteRejected() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 0, false, Set.of(), Map.of(),
                604800000L, Set.of()); // 7 days
        assertTrue(validator.validate(p,
                req("t", 1, (short) 1, Map.of("retention.ms", "86400000"), null)).valid());
        assertTrue(hasRule(validator.validate(p,
                req("t", 1, (short) 1, Map.of("retention.ms", "999999999999"), null)),
                "RETENTION_TOO_LONG"));
        // -1 = infinite retention, rejected when a cap exists
        assertTrue(hasRule(validator.validate(p,
                req("t", 1, (short) 1, Map.of("retention.ms", "-1"), null)),
                "RETENTION_TOO_LONG"));
    }

    @Test
    void nonNumericRetentionReported() {
        TopicPolicy p = new TopicPolicy(null, 0, 0, 0, false, Set.of(), Map.of(),
                604800000L, Set.of());
        assertTrue(hasRule(validator.validate(p,
                req("t", 1, (short) 1, Map.of("retention.ms", "forever"), null)),
                "RETENTION_NOT_NUMERIC"));
    }

    @Test
    void allViolationsCollectedTogether() {
        TopicPolicy p = new TopicPolicy("orders\\..+", 6, 12, 3, true,
                Set.of("retention.ms"), Map.of(), 0L, Set.of());
        ValidationResult r = validator.validate(p,
                req("bad", 1, (short) 1, Map.of(), null));
        assertFalse(r.valid());
        // name + partitions too few + replication + owner + missing config = 5
        assertEquals(5, r.violations().size());
    }
}
