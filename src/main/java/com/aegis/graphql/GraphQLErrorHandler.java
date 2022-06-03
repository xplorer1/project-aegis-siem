package com.aegis.graphql;

import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.schema.DataFetchingEnvironment;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.stereotype.Component;

import com.aegis.query.QueryExecutionException;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom GraphQL error handler that formats exceptions into GraphQL error responses.
 * 
 * This handler intercepts exceptions thrown during GraphQL query execution and
 * transforms them into properly formatted GraphQL errors with appropriate error types,
 * messages, and extensions.
 * 
 * Supported error types:
 * - BAD_REQUEST: Invalid query syntax or parameters
 * - INTERNAL_ERROR: Unexpected server errors
 * - NOT_FOUND: Requested resource not found
 * - UNAUTHORIZED: Authentication required
 * - FORBIDDEN: Insufficient permissions
 */
@Component
public class GraphQLErrorHandler extends DataFetcherExceptionResolverAdapter {

    /**
     * Resolves exceptions to GraphQL errors with appropriate error types and extensions.
     * 
     * @param ex the exception thrown during data fetching
     * @param env the data fetching environment containing query context
     * @return a GraphQL error with formatted message and extensions
     */
    @Override
    protected GraphQLError resolveToSingleError(Throwable ex, DataFetchingEnvironment env) {
        // Handle custom GraphQL exceptions
        if (ex instanceof GraphQLException) {
            return handleGraphQLException((GraphQLException) ex, env);
        }
        
        // Handle query execution exceptions
        if (ex instanceof QueryExecutionException) {
            return handleQueryExecutionException((QueryExecutionException) ex, env);
        }
        
        // Handle illegal argument exceptions (validation errors)
        if (ex instanceof IllegalArgumentException) {
            return handleIllegalArgumentException((IllegalArgumentException) ex, env);
        }
        
        // Handle null pointer exceptions
        if (ex instanceof NullPointerException) {
            return handleNullPointerException((NullPointerException) ex, env);
        }
        
        // Handle all other exceptions as internal errors
        return handleGenericException(ex, env);
    }

    /**
     * Handles custom GraphQLException with its specific error type and code.
     */
    private GraphQLError handleGraphQLException(GraphQLException ex, DataFetchingEnvironment env) {
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("errorCode", ex.getErrorCode());
        
        if (ex.getCause() != null) {
            extensions.put("cause", ex.getCause().getClass().getSimpleName());
        }
        
        return GraphqlErrorBuilder.newError(env)
                .errorType(ex.getErrorType())
                .message(formatErrorMessage(ex))
                .extensions(extensions)
                .build();
    }

    /**
     * Handles QueryExecutionException by creating a BAD_REQUEST error.
     */
    private GraphQLError handleQueryExecutionException(QueryExecutionException ex, DataFetchingEnvironment env) {
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("errorCode", "QUERY_EXECUTION_ERROR");
        extensions.put("tier", ex.getTier() != null ? ex.getTier().name() : "UNKNOWN");
        
        if (ex.getCause() != null) {
            extensions.put("cause", ex.getCause().getClass().getSimpleName());
        }
        
        return GraphqlErrorBuilder.newError(env)
                .errorType(ErrorType.BAD_REQUEST)
                .message(formatErrorMessage(ex))
                .extensions(extensions)
                .build();
    }

    /**
     * Handles IllegalArgumentException by creating a BAD_REQUEST error.
     */
    private GraphQLError handleIllegalArgumentException(IllegalArgumentException ex, DataFetchingEnvironment env) {
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("errorCode", "INVALID_ARGUMENT");
        
        return GraphqlErrorBuilder.newError(env)
                .errorType(ErrorType.BAD_REQUEST)
                .message(formatErrorMessage(ex))
                .extensions(extensions)
                .build();
    }

    /**
     * Handles NullPointerException by creating an INTERNAL_ERROR.
     */
    private GraphQLError handleNullPointerException(NullPointerException ex, DataFetchingEnvironment env) {
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("errorCode", "NULL_POINTER");
        
        return GraphqlErrorBuilder.newError(env)
                .errorType(ErrorType.INTERNAL_ERROR)
                .message("An unexpected null value was encountered")
                .extensions(extensions)
                .build();
    }

    /**
     * Handles generic exceptions by creating an INTERNAL_ERROR.
     */
    private GraphQLError handleGenericException(Throwable ex, DataFetchingEnvironment env) {
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("errorCode", "INTERNAL_ERROR");
        extensions.put("exceptionType", ex.getClass().getSimpleName());
        
        return GraphqlErrorBuilder.newError(env)
                .errorType(ErrorType.INTERNAL_ERROR)
                .message("An internal error occurred while processing your request")
                .extensions(extensions)
                .build();
    }

    /**
     * Formats error messages to be user-friendly while preserving important details.
     * 
     * @param ex the exception to format
     * @return a formatted error message
     */
    private String formatErrorMessage(Throwable ex) {
        String message = ex.getMessage();
        
        // If no message, use exception type
        if (message == null || message.isEmpty()) {
            return "An error occurred: " + ex.getClass().getSimpleName();
        }
        
        // Sanitize message to remove sensitive information
        // Remove stack traces and internal paths
        message = message.split("\n")[0]; // Take only first line
        message = message.replaceAll("at .*", ""); // Remove stack trace references
        
        return message.trim();
    }
}
