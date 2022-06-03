package com.aegis.graphql;

import com.aegis.domain.StorageTier;
import com.aegis.query.QueryExecutionException;
import graphql.GraphQLError;
import graphql.schema.DataFetchingEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.graphql.execution.ErrorType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for GraphQLErrorHandler.
 * 
 * Tests error handling and formatting for various exception types.
 */
class GraphQLErrorHandlerTest {

    private GraphQLErrorHandler errorHandler;
    private DataFetchingEnvironment mockEnv;

    @BeforeEach
    void setUp() {
        errorHandler = new GraphQLErrorHandler();
        mockEnv = mock(DataFetchingEnvironment.class);
    }

    @Test
    void testHandleQueryExecutionException() {
        // Given
        QueryExecutionException exception = new QueryExecutionException(
                "Query failed on hot tier",
                StorageTier.HOT,
                new RuntimeException("Connection timeout")
        );

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).contains("Query failed on hot tier");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        assertThat(error.getExtensions()).containsEntry("errorCode", "QUERY_EXECUTION_ERROR");
        assertThat(error.getExtensions()).containsEntry("tier", "HOT");
        assertThat(error.getExtensions()).containsEntry("cause", "RuntimeException");
    }

    @Test
    void testHandleIllegalArgumentException() {
        // Given
        IllegalArgumentException exception = new IllegalArgumentException("Invalid query parameter");

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).isEqualTo("Invalid query parameter");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
        assertThat(error.getExtensions()).containsEntry("errorCode", "INVALID_ARGUMENT");
    }

    @Test
    void testHandleNullPointerException() {
        // Given
        NullPointerException exception = new NullPointerException("Unexpected null value");

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).isEqualTo("An unexpected null value was encountered");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
        assertThat(error.getExtensions()).containsEntry("errorCode", "NULL_POINTER");
    }

    @Test
    void testHandleGenericException() {
        // Given
        RuntimeException exception = new RuntimeException("Something went wrong");

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).isEqualTo("An internal error occurred while processing your request");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
        assertThat(error.getExtensions()).containsEntry("errorCode", "INTERNAL_ERROR");
        assertThat(error.getExtensions()).containsEntry("exceptionType", "RuntimeException");
    }

    @Test
    void testHandleCustomGraphQLException() {
        // Given
        GraphQLException exception = new GraphQLException(
                "Resource not found",
                ErrorType.NOT_FOUND,
                "RESOURCE_NOT_FOUND"
        );

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).isEqualTo("Resource not found");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.NOT_FOUND);
        assertThat(error.getExtensions()).containsEntry("errorCode", "RESOURCE_NOT_FOUND");
    }

    @Test
    void testHandleExceptionWithNullMessage() {
        // Given
        RuntimeException exception = new RuntimeException((String) null);

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).contains("RuntimeException");
    }

    @Test
    void testHandleExceptionWithMultilineMessage() {
        // Given
        RuntimeException exception = new RuntimeException(
                "Error on line 1\nStack trace line 2\nat com.example.Class.method"
        );

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        // Should only contain first line, sanitized
        assertThat(error.getMessage()).doesNotContain("\n");
        assertThat(error.getMessage()).doesNotContain("at com.example");
    }

    @Test
    void testHandleQueryExecutionExceptionWithoutCause() {
        // Given
        QueryExecutionException exception = new QueryExecutionException(
                "Query timeout",
                StorageTier.WARM,
                null
        );

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).contains("Query timeout");
        assertThat(error.getExtensions()).containsEntry("tier", "WARM");
        assertThat(error.getExtensions()).doesNotContainKey("cause");
    }

    @Test
    void testHandleGraphQLExceptionWithCause() {
        // Given
        GraphQLException exception = new GraphQLException(
                "Database connection failed",
                ErrorType.INTERNAL_ERROR,
                new IllegalStateException("Connection pool exhausted")
        );

        // When
        GraphQLError error = errorHandler.resolveToSingleError(exception, mockEnv);

        // Then
        assertThat(error).isNotNull();
        assertThat(error.getMessage()).contains("Database connection failed");
        assertThat(error.getErrorType()).isEqualTo(ErrorType.INTERNAL_ERROR);
        assertThat(error.getExtensions()).containsEntry("cause", "IllegalStateException");
    }
}
