package com.emirates.helix.macs;

public class ConverterException extends RuntimeException {
    public ConverterException(String message) { super(message); }
    public ConverterException(String message, Throwable cause) { super(message, cause); }
    public ConverterException(Throwable cause) { super(cause); }
}
