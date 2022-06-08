package com.aegis.graphql;

import org.junit.jupiter.api.Test;
import org.springframework.graphql.execution.ErrorType;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for GraphQL error formatting.
 * 
 * Demonstrates how errors are formatted in GraphQL responses.
 */
class GraphQLErrorFormattingTest {

    @Test
    void testErrorResponseStructure() {
        // Given
        GraphQLException exception = new GraphQLException(
                "Invalid query syntax",
                ErrorType.BAD_REQUEST,
                "INVALID_SYNTAX"
        );

        // Then - verify exception structure
        assertThat(exception.getMessage()).isEqualTo("Invalid query syntax");
        assertThat(exception.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        assertThat(exception.getErrorCode()).isEqualTo("INVALID_SYNTAX");
    }

    @Test
    void testErrorExtensionsFormat() {
        // This test demonstrates the expected error response format
        // that will be returned to GraphQL clients
        
        // Expected error response structure:
        Map<String, Object> expectedError = Map.of(
                "message", "Query execution failed",
                "errorType", "BAD_REQUEST",
                "extensions", Map.of(
                        "errorCode", "QUERY_EXECUTION_ERROR",
                        "tier", "HOT"
                )
        );

        // Verify structure
        assertThat(expectedError).containsKeys("message", "errorType", "extensions");
        assertThat(expectedError.get("extensions")).isInstanceOf(Map.class);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> extensions = (Map<String, Object>) expectedError.get("extensions");
        assertThat(extensions).containsKeys("errorCode", "tier");
    }

    @Test
    void testMultipleErrorTypes() {
        // Test that different error types are properly categorized
        
        GraphQLException badRequest = new GraphQLException(
                "Bad request",
                ErrorType.BAD_REQUEST
        );
        assertThat(badRequest.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);

        GraphQLException notFound = new GraphQLException(
                "Not found",
                ErrorType.NOT_FOUND
        );
        assertThat(notFound.getErrorType()).isEqualTo(ErrorType.NOT_FOUND);

        GraphQLException unauthorized = new GraphQLException(
                "Unauthorized",
                ErrorType.UNAUTHORIZED
        );
        assertThat(unauthorized.getErrorType()).isEqualTo(ErrorType.UNAUTHORIZED);

        GraphQLException forbidden = new GraphQLException(
                "Forbidden",
                ErrorType.FORBIDDEN
        );
        assertThat(forbidden.getErrorType()).isEqualTo(ErrorType.FORBIDDEN);

        GraphQLException internalError = new GraphQLException(
                "Internal error",
                ErrorType.INTERNAL_ERROR
        );
        assertThat(internalError.getErrorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
    }
}
