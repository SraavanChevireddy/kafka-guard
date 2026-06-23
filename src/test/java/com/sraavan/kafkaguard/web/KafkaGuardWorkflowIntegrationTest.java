package com.sraavan.kafkaguard.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class KafkaGuardWorkflowIntegrationTest {

    @Autowired
    private MockMvc mvc;

    private static final String STRICT_POLICY = """
            { "namePattern": "(orders|payments)\\\\.[a-z0-9_]+",
              "minPartitions": 3, "maxPartitions": 24, "minReplicationFactor": 3,
              "requireOwner": true, "requiredConfigs": ["retention.ms"],
              "allowedConfigValues": { "cleanup.policy": ["delete", "compact"] },
              "maxRetentionMs": 604800000, "forbiddenConfigs": ["segment.bytes"] }""";

    private void setPolicy(String cluster, String policyJson) throws Exception {
        mvc.perform(put("/api/clusters/{c}/policy", cluster).contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"updatedBy\": \"admin\", \"policy\": " + policyJson + " }"))
                .andExpect(status().isOk());
    }

    @Test
    void validTopicIsQueuedThenApproved() throws Exception {
        setPolicy("prod", STRICT_POLICY);

        MvcResult res = mvc.perform(post("/api/clusters/prod/requests")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                { "requestedBy": "alice", "topic": {
                                    "name": "orders.created", "partitions": 6, "replicationFactor": 3,
                                    "configs": { "retention.ms": "86400000", "cleanup.policy": "delete" },
                                    "owner": "team-orders"
                                }}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status", is("PENDING")))
                .andReturn();

        String id = com.jayway.jsonpath.JsonPath.read(res.getResponse().getContentAsString(), "$.id")
                .toString();
        mvc.perform(post("/api/requests/{id}/approve", id).contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"approver\": \"bob\" }"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status", is("APPROVED")));
    }

    @Test
    void invalidTopicIsRejectedWith422AndRecorded() throws Exception {
        setPolicy("prod2", STRICT_POLICY);

        mvc.perform(post("/api/clusters/prod2/requests").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                { "requestedBy": "alice", "topic": {
                                    "name": "BadName", "partitions": 1, "replicationFactor": 1,
                                    "configs": {}, "owner": ""
                                }}"""))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status", is("REJECTED_INVALID")));

        mvc.perform(get("/api/clusters/prod2/requests"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status", is("REJECTED_INVALID")));
    }

    @Test
    void validateIsDryRunAndDoesNotRecord() throws Exception {
        setPolicy("prod3", STRICT_POLICY);

        mvc.perform(post("/api/clusters/prod3/validate").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                { "topic": {
                                    "name": "payments.settled", "partitions": 6, "replicationFactor": 3,
                                    "configs": { "retention.ms": "3600000" }, "owner": "team-pay"
                                }}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid", is(true)));

        mvc.perform(get("/api/clusters/prod3/requests"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()", is(0)));
    }

    @Test
    void wildcardDefaultPolicyAppliesToUnknownCluster() throws Exception {
        setPolicy("*", STRICT_POLICY);

        mvc.perform(post("/api/clusters/some-new-cluster/validate")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                { "topic": {
                                    "name": "nope", "partitions": 1, "replicationFactor": 1,
                                    "configs": {}, "owner": ""
                                }}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid", is(false)));
    }

    @Test
    void approvingAnAlreadyDecidedRequestConflicts() throws Exception {
        setPolicy("prod4", STRICT_POLICY);
        MvcResult res = mvc.perform(post("/api/clusters/prod4/requests")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                { "requestedBy": "alice", "topic": {
                                    "name": "orders.x", "partitions": 6, "replicationFactor": 3,
                                    "configs": { "retention.ms": "86400000" }, "owner": "team-orders"
                                }}"""))
                .andExpect(status().isCreated())
                .andReturn();
        String id = com.jayway.jsonpath.JsonPath.read(res.getResponse().getContentAsString(), "$.id")
                .toString();

        mvc.perform(post("/api/requests/{id}/approve", id).contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"approver\": \"bob\" }")).andExpect(status().isOk());
        mvc.perform(post("/api/requests/{id}/reject", id).contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"approver\": \"bob\" }")).andExpect(status().isConflict());
    }

    @Test
    void decidingUnknownRequestReturns404() throws Exception {
        mvc.perform(post("/api/requests/99999/approve").contentType(MediaType.APPLICATION_JSON)
                        .content("{ \"approver\": \"bob\" }"))
                .andExpect(status().isNotFound());
    }
}
