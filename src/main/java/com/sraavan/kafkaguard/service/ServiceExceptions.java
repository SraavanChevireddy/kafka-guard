package com.sraavan.kafkaguard.service;

/** Domain exceptions mapped to HTTP status codes. */
public final class ServiceExceptions {

    private ServiceExceptions() {
    }

    /** 404 — referenced request or policy does not exist. */
    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    /** 409 — request conflicts with current state (e.g. deciding an already-decided request). */
    public static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }
}
