package com.sraavan.kafkaguard.policy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Serializes {@link TopicPolicy} to/from the JSON stored in the database and exposed on the API. */
public class PolicyCodec {

    private final ObjectMapper mapper;

    public PolicyCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize: " + e.getMessage());
        }
    }

    public TopicPolicy fromJson(String json) {
        try {
            return mapper.readValue(json, TopicPolicy.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid policy JSON: " + e.getMessage());
        }
    }
}
