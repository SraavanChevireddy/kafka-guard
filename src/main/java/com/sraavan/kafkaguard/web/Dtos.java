package com.sraavan.kafkaguard.web;

import com.sraavan.kafkaguard.domain.TopicRequestRecord;
import com.sraavan.kafkaguard.policy.TopicPolicy;
import com.sraavan.kafkaguard.policy.TopicRequest;
import com.sraavan.kafkaguard.policy.ValidationResult;

import java.time.Instant;
import java.util.List;

/** API request/response payloads. */
public final class Dtos {

    private Dtos() {
    }

    public record UpsertPolicyRequest(String updatedBy, TopicPolicy policy) {
    }

    public record PolicyResponse(String cluster, TopicPolicy policy) {
    }

    public record SubmitRequest(String requestedBy, TopicRequest topic) {
    }

    public record ValidationResponse(boolean valid, List<ValidationResult.Violation> violations) {
        public static ValidationResponse from(ValidationResult r) {
            return new ValidationResponse(r.valid(), r.violations());
        }
    }

    public record DecisionRequest(String approver) {
    }

    public record RequestView(Long id, String cluster, String topicName, String status,
                              String requestedBy, Instant requestedAt,
                              String decidedBy, Instant decidedAt) {
        public static RequestView from(TopicRequestRecord r) {
            return new RequestView(r.getId(), r.getCluster(), r.getTopicName(), r.getStatus().name(),
                    r.getRequestedBy(), r.getRequestedAt(), r.getDecidedBy(), r.getDecidedAt());
        }
    }
}
