package com.sraavan.kafkaguard.service;

import com.sraavan.kafkaguard.domain.ClusterPolicy;
import com.sraavan.kafkaguard.domain.TopicRequestRecord;
import com.sraavan.kafkaguard.policy.PolicyCodec;
import com.sraavan.kafkaguard.policy.TopicPolicy;
import com.sraavan.kafkaguard.policy.TopicPolicyValidator;
import com.sraavan.kafkaguard.policy.TopicRequest;
import com.sraavan.kafkaguard.policy.ValidationResult;
import com.sraavan.kafkaguard.repo.ClusterPolicyRepository;
import com.sraavan.kafkaguard.repo.TopicRequestRepository;
import com.sraavan.kafkaguard.service.ServiceExceptions.ConflictException;
import com.sraavan.kafkaguard.service.ServiceExceptions.NotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Governance core for Kafka topic provisioning.
 *
 * <p>A topic request is validated against the cluster's {@link TopicPolicy}. If it fails
 * validation it is recorded as {@code REJECTED_INVALID} and never enters the approval
 * queue. If it passes it becomes {@code PENDING}, awaiting a human approve/reject. Every
 * outcome is persisted for audit.
 *
 * <p>The wildcard cluster id {@code *} holds a default policy used when a cluster has none.
 */
@Service
public class KafkaGuardService {

    public static final String DEFAULT_POLICY_ID = "*";

    private final ClusterPolicyRepository policies;
    private final TopicRequestRepository requests;
    private final PolicyCodec codec;
    private final TopicPolicyValidator validator = new TopicPolicyValidator();

    public KafkaGuardService(ClusterPolicyRepository policies, TopicRequestRepository requests,
                             PolicyCodec codec) {
        this.policies = policies;
        this.requests = requests;
        this.codec = codec;
    }

    @Transactional
    public ClusterPolicy upsertPolicy(String cluster, TopicPolicy policy, String updatedBy) {
        String json = codec.toJson(policy);
        return policies.findByCluster(cluster)
                .map(existing -> {
                    existing.update(json, updatedBy);
                    return policies.save(existing);
                })
                .orElseGet(() -> policies.save(new ClusterPolicy(cluster, json, updatedBy)));
    }

    @Transactional(readOnly = true)
    public TopicPolicy effectivePolicy(String cluster) {
        return policies.findByCluster(cluster)
                .or(() -> policies.findByCluster(DEFAULT_POLICY_ID))
                .map(p -> codec.fromJson(p.getPolicyJson()))
                .orElseGet(TopicPolicy::permissive);
    }

    /** Validate without recording anything — the dry-run a CI job or UI calls first. */
    @Transactional(readOnly = true)
    public ValidationResult validate(String cluster, TopicRequest req) {
        return validator.validate(effectivePolicy(cluster), req);
    }

    /**
     * Submit a topic request. Invalid requests are stored as {@code REJECTED_INVALID};
     * valid ones become {@code PENDING}.
     */
    @Transactional
    public TopicRequestRecord submit(String cluster, TopicRequest req, String requestedBy) {
        ValidationResult result = validator.validate(effectivePolicy(cluster), req);
        String requestJson = codec.toJson(req);

        if (!result.valid()) {
            String violations = codec.toJson(result.violations());
            return requests.save(new TopicRequestRecord(cluster, req.name(), requestJson,
                    TopicRequestRecord.Status.REJECTED_INVALID, violations, requestedBy));
        }
        return requests.save(new TopicRequestRecord(cluster, req.name(), requestJson,
                TopicRequestRecord.Status.PENDING, null, requestedBy));
    }

    @Transactional
    public TopicRequestRecord approve(long id, String approver) {
        TopicRequestRecord r = require(id);
        if (r.getStatus() != TopicRequestRecord.Status.PENDING) {
            throw new ConflictException("Request " + id + " is " + r.getStatus()
                    + ", only PENDING requests can be approved");
        }
        r.approve(approver);
        return requests.save(r);
    }

    @Transactional
    public TopicRequestRecord reject(long id, String approver) {
        TopicRequestRecord r = require(id);
        if (r.getStatus() != TopicRequestRecord.Status.PENDING) {
            throw new ConflictException("Request " + id + " is " + r.getStatus()
                    + ", only PENDING requests can be rejected");
        }
        r.reject(approver);
        return requests.save(r);
    }

    @Transactional(readOnly = true)
    public List<TopicRequestRecord> history(String cluster) {
        return requests.findByClusterOrderByRequestedAtDesc(cluster);
    }

    private TopicRequestRecord require(long id) {
        return requests.findById(id)
                .orElseThrow(() -> new NotFoundException("No topic request with id " + id));
    }
}
