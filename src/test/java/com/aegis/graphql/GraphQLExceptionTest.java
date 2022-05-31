package com.aegis.graphql;

import org.junit.jupiter.api.Test;
import org.springframework.graphql.execution.ErrorType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for GraphQLException.
 */
class GraphQLExceptionTest {

    @Test
    void testConstructorWithMessageAndErrorType() {
        // Given
        String message = "Test error message";
        ErrorType errorType = ErrorType.BAD_REQUEST;

        // When
        GraphQLException exception = new GraphQLException(message, errorType);

        // Then
        assertThat(exception.getMessage()).isEqualTo(message);
        assertThat(exception.getErrorType()).isEqualTo(errorType);
        assertThat(exception.getErrorCode()).isEqualTo("BAD_REQUEST");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void testConstructorWithMessageErrorTypeAndCode() {
        // Given
        String message = "Custom error";
        ErrorType errorType = ErrorType.NOT_FOUND;
        String errorCode = "RESOURCE_NOT_FOUND";

        // When
        GraphQLException exception = new GraphQLException(message, errorType, errorCode);

        // Then
        assertThat(exception.getMessage()).isEqualTo(message);
        assertThat(exception.getErrorType()).isEqualTo(errorType);
        assertThat(exception.getErrorCode()).isEqualTo(errorCode);
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void testConstructorWithMessageErrorTypeAndCause() {
        // Given
        String message = "Wrapped error";
        ErrorType errorType = ErrorType.INTERNAL_ERROR;
        Throwable cause = new RuntimeException("Original cause");

        // When
        GraphQLException exception = new GraphQLException(message, errorType, cause);

        // Then
        assertThat(exception.getMessage()).isEqualTo(message);
        assertThat(exception.getErrorType()).isEqualTo(errorType);
        assertThat(exception.getErrorCode()).isEqualTo("INTERNAL_ERROR");
        assertThat(exception.getCause()).isEqualTo(cause);
    }

    @Test
    void testErrorTypeMapping() {
        // Test all error types
        ErrorType[] errorTypes = {
            ErrorType.BAD_REQUEST,
            ErrorType.UNAUTHORIZED,
            ErrorType.FORBIDDEN,
            ErrorType.NOT_FOUND,
            ErrorType.INTERNAL_ERROR
        };

        for (ErrorType errorType : errorTypes) {
            GraphQLException exception = new GraphQLException("Test", errorType);
            assertThat(exception.getErrorType()).isEqualTo(errorType);
            assertThat(exception.getErrorCode()).isEqualTo(errorType.name());
        }
    }
}
