package com.aegis.graphql;

import org.springframework.graphql.execution.ErrorType;

/**
 * Custom exception for GraphQL-specific errors.
 * 
 * This exception allows throwing errors with specific GraphQL error types
 * and additional context that will be properly formatted by the error handler.
 */
public class GraphQLException extends RuntimeException {
    
    private final ErrorType errorType;
    private final String errorCode;
    
    /**
     * Creates a new GraphQL exception with a message and error type.
     * 
     * @param message the error message
     * @param errorType the GraphQL error type
     */
    public GraphQLException(String message, ErrorType errorType) {
        super(message);
        this.errorType = errorType;
        this.errorCode = errorType.name();
    }
    
    /**
     * Creates a new GraphQL exception with a message, error type, and custom error code.
     * 
     * @param message the error message
     * @param errorType the GraphQL error type
     * @param errorCode a custom error code for client handling
     */
    public GraphQLException(String message, ErrorType errorType, String errorCode) {
        super(message);
        this.errorType = errorType;
        this.errorCode = errorCode;
    }
    
    /**
     * Creates a new GraphQL exception with a message, error type, and cause.
     * 
     * @param message the error message
     * @param errorType the GraphQL error type
     * @param cause the underlying cause
     */
    public GraphQLException(String message, ErrorType errorType, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.errorCode = errorType.name();
    }
    
    /**
     * Gets the GraphQL error type.
     * 
     * @return the error type
     */
    public ErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Gets the custom error code.
     * 
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }
}
