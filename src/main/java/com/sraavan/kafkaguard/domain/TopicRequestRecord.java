package com.sraavan.kafkaguard.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * A submitted topic-provisioning request and its lifecycle. Persisted both as a workflow
 * record and as an audit entry: the request payload, the validation outcome, and the
 * eventual approve/reject decision are all retained.
 */
@Entity
@Table(name = "topic_request",
        indexes = @Index(name = "idx_treq_cluster", columnList = "cluster"))
public class TopicRequestRecord {

    public enum Status {
        PENDING,
        APPROVED,
        REJECTED,
        REJECTED_INVALID
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String cluster;

    @Column(name = "topic_name", nullable = false)
    private String topicName;

    @Lob
    @Column(name = "request_json", nullable = false)
    private String requestJson;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Lob
    @Column(name = "violations")
    private String violations;

    @Column(nullable = false)
    private String requestedBy;

    @Column(nullable = false)
    private Instant requestedAt = Instant.now();

    private String decidedBy;

    private Instant decidedAt;

    protected TopicRequestRecord() {
    }

    public TopicRequestRecord(String cluster, String topicName, String requestJson,
                              Status status, String violations, String requestedBy) {
        this.cluster = cluster;
        this.topicName = topicName;
        this.requestJson = requestJson;
        this.status = status;
        this.violations = violations;
        this.requestedBy = requestedBy;
        this.requestedAt = Instant.now();
    }

    public void approve(String approver) {
        this.status = Status.APPROVED;
        this.decidedBy = approver;
        this.decidedAt = Instant.now();
    }

    public void reject(String approver) {
        this.status = Status.REJECTED;
        this.decidedBy = approver;
        this.decidedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getCluster() {
        return cluster;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getRequestJson() {
        return requestJson;
    }

    public Status getStatus() {
        return status;
    }

    public String getViolations() {
        return violations;
    }

    public String getRequestedBy() {
        return requestedBy;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public String getDecidedBy() {
        return decidedBy;
    }

    public Instant getDecidedAt() {
        return decidedAt;
    }
}
