package com.sraavan.kafkaguard.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * The governance policy for a named Kafka cluster (or environment). Stored as JSON so the
 * ruleset can evolve without migrations. The id {@code *} holds a default policy.
 */
@Entity
@Table(name = "cluster_policy")
public class ClusterPolicy {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "cluster", nullable = false, unique = true)
    private String cluster;

    @Lob
    @Column(name = "policy_json", nullable = false)
    private String policyJson;

    @Column(nullable = false)
    private String updatedBy;

    @Column(nullable = false)
    private Instant updatedAt = Instant.now();

    protected ClusterPolicy() {
    }

    public ClusterPolicy(String cluster, String policyJson, String updatedBy) {
        this.cluster = cluster;
        this.policyJson = policyJson;
        this.updatedBy = updatedBy;
        this.updatedAt = Instant.now();
    }

    public void update(String policyJson, String updatedBy) {
        this.policyJson = policyJson;
        this.updatedBy = updatedBy;
        this.updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getCluster() {
        return cluster;
    }

    public String getPolicyJson() {
        return policyJson;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
