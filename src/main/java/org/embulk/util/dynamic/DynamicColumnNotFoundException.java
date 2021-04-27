package org.embulk.util.dynamic;

public class DynamicColumnNotFoundException extends RuntimeException {
    public DynamicColumnNotFoundException(String message) {
        super(message);
    }
}
