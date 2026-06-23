package com.sraavan.kafkaguard.policy;

import java.util.List;

/**
 * Outcome of validating a {@link TopicRequest} against a {@link TopicPolicy}.
 *
 * @param valid      whether the request satisfies every enforced rule
 * @param violations the reasons it failed (empty when valid)
 */
public record ValidationResult(boolean valid, List<Violation> violations) {

    public ValidationResult {
        violations = List.copyOf(violations);
    }

    public static ValidationResult ok() {
        return new ValidationResult(true, List.of());
    }

    public static ValidationResult failed(List<Violation> violations) {
        return new ValidationResult(false, violations);
    }

    /**
     * @param rule    machine-readable id (e.g. {@code NAME_PATTERN})
     * @param message human-readable explanation
     */
    public record Violation(String rule, String message) {
    }
}
