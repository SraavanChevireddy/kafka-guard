package com.sraavan.kafkaguard.web;

import com.sraavan.kafkaguard.domain.TopicRequestRecord;
import com.sraavan.kafkaguard.service.KafkaGuardService;
import com.sraavan.kafkaguard.web.Dtos.DecisionRequest;
import com.sraavan.kafkaguard.web.Dtos.PolicyResponse;
import com.sraavan.kafkaguard.web.Dtos.RequestView;
import com.sraavan.kafkaguard.web.Dtos.SubmitRequest;
import com.sraavan.kafkaguard.web.Dtos.UpsertPolicyRequest;
import com.sraavan.kafkaguard.web.Dtos.ValidationResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * kafka-guard governance API.
 *
 * <pre>
 * PUT  /api/clusters/{cluster}/policy      set/update the topic policy ("*" for default)
 * GET  /api/clusters/{cluster}/policy      view the effective policy
 * POST /api/clusters/{cluster}/validate    dry-run validate a topic request (no write)
 * POST /api/clusters/{cluster}/requests    submit a topic request (validated + queued)
 * GET  /api/clusters/{cluster}/requests    list request history
 * POST /api/requests/{id}/approve          approve a pending request
 * POST /api/requests/{id}/reject           reject a pending request
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class KafkaGuardController {

    private final KafkaGuardService service;

    public KafkaGuardController(KafkaGuardService service) {
        this.service = service;
    }

    @PutMapping("/clusters/{cluster}/policy")
    public PolicyResponse upsertPolicy(@PathVariable String cluster,
                                       @RequestBody UpsertPolicyRequest req) {
        service.upsertPolicy(cluster, req.policy(), req.updatedBy());
        return new PolicyResponse(cluster, service.effectivePolicy(cluster));
    }

    @GetMapping("/clusters/{cluster}/policy")
    public PolicyResponse getPolicy(@PathVariable String cluster) {
        return new PolicyResponse(cluster, service.effectivePolicy(cluster));
    }

    @PostMapping("/clusters/{cluster}/validate")
    public ValidationResponse validate(@PathVariable String cluster, @RequestBody SubmitRequest req) {
        return ValidationResponse.from(service.validate(cluster, req.topic()));
    }

    @PostMapping("/clusters/{cluster}/requests")
    public ResponseEntity<RequestView> submit(@PathVariable String cluster,
                                              @RequestBody SubmitRequest req) {
        TopicRequestRecord r = service.submit(cluster, req.topic(), req.requestedBy());
        HttpStatus status = r.getStatus() == TopicRequestRecord.Status.REJECTED_INVALID
                ? HttpStatus.UNPROCESSABLE_ENTITY : HttpStatus.CREATED;
        return ResponseEntity.status(status).body(RequestView.from(r));
    }

    @GetMapping("/clusters/{cluster}/requests")
    public List<RequestView> history(@PathVariable String cluster) {
        return service.history(cluster).stream().map(RequestView::from).toList();
    }

    @PostMapping("/requests/{id}/approve")
    public RequestView approve(@PathVariable long id, @RequestBody DecisionRequest req) {
        return RequestView.from(service.approve(id, req.approver()));
    }

    @PostMapping("/requests/{id}/reject")
    public RequestView reject(@PathVariable long id, @RequestBody DecisionRequest req) {
        return RequestView.from(service.reject(id, req.approver()));
    }
}
