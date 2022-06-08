# GraphQL Error Handling

This package provides custom error handling for the AEGIS SIEM GraphQL API.

## Overview

The GraphQL error handling system transforms Java exceptions into properly formatted GraphQL error responses with appropriate error types, messages, and extensions.

## Components

### GraphQLErrorHandler

The main error handler that intercepts exceptions during GraphQL query execution and formats them into GraphQL errors.

**Supported Error Types:**
- `BAD_REQUEST`: Invalid query syntax or parameters
- `INTERNAL_ERROR`: Unexpected server errors
- `NOT_FOUND`: Requested resource not found
- `UNAUTHORIZED`: Authentication required
- `FORBIDDEN`: Insufficient permissions

**Exception Handling:**
- `QueryExecutionException`: Mapped to BAD_REQUEST with tier information
- `IllegalArgumentException`: Mapped to BAD_REQUEST for validation errors
- `NullPointerException`: Mapped to INTERNAL_ERROR
- `GraphQLException`: Custom exception with specific error type
- Generic exceptions: Mapped to INTERNAL_ERROR

### GraphQLException

Custom exception class for GraphQL-specific errors that allows specifying:
- Error message
- GraphQL error type
- Custom error code
- Underlying cause

### GraphQLConfig

Configuration class that registers custom scalar types:
- `DateTime`: ISO 8601 timestamp format
- `JSON`: Arbitrary JSON data
- `Object`: Alias for JSON scalar

## Error Response Format

GraphQL errors are returned in the following format:

```json
{
  "errors": [
    {
      "message": "Query execution failed",
      "errorType": "BAD_REQUEST",
      "extensions": {
        "errorCode": "QUERY_EXECUTION_ERROR",
        "tier": "HOT",
        "cause": "RuntimeException"
      },
      "path": ["searchEvents"],
      "locations": [{"line": 2, "column": 3}]
    }
  ],
  "data": null
}
```

## Usage Examples

### Throwing Custom GraphQL Exceptions

```java
@QueryMapping
public Flux<Event> searchEvents(@Argument String query) {
    if (query == null || query.isEmpty()) {
        throw new GraphQLException(
            "Query parameter is required",
            ErrorType.BAD_REQUEST,
            "MISSING_QUERY"
        );
    }
    
    // Execute query...
}
```

### Handling Query Execution Errors

```java
try {
    QueryPlan plan = transpiler.transpile(aql);
    return queryExecutor.execute(plan);
} catch (QueryExecutionException ex) {
    // Automatically handled by GraphQLErrorHandler
    // Returns BAD_REQUEST with tier and cause information
    throw ex;
}
```

### Validation Errors

```java
@MutationMapping
public Mono<Alert> acknowledgeAlert(@Argument String alertId) {
    if (alertId == null) {
        throw new IllegalArgumentException("Alert ID is required");
        // Automatically handled as BAD_REQUEST
    }
    
    return alertRepository.findById(alertId)
        .switchIfEmpty(Mono.error(
            new GraphQLException(
                "Alert not found: " + alertId,
                ErrorType.NOT_FOUND,
                "ALERT_NOT_FOUND"
            )
        ))
        .flatMap(alert -> {
            alert.setStatus(AlertStatus.ACKNOWLEDGED);
            return alertRepository.save(alert);
        });
}
```

## Error Codes

The following error codes are used in the `extensions.errorCode` field:

| Error Code | Description | Error Type |
|------------|-------------|------------|
| `QUERY_EXECUTION_ERROR` | Query failed during execution | BAD_REQUEST |
| `INVALID_ARGUMENT` | Invalid argument provided | BAD_REQUEST |
| `NULL_POINTER` | Unexpected null value | INTERNAL_ERROR |
| `INTERNAL_ERROR` | Generic internal error | INTERNAL_ERROR |
| Custom codes | Application-specific errors | Varies |

## Testing

The error handling system includes comprehensive unit tests:

- `GraphQLErrorHandlerTest`: Tests error handler for various exception types
- `GraphQLExceptionTest`: Tests custom exception class
- `GraphQLErrorFormattingTest`: Integration tests for error response format

Run tests with:
```bash
./gradlew test --tests "com.aegis.graphql.*"
```

## Security Considerations

The error handler sanitizes error messages to prevent information leakage:
- Stack traces are removed from error messages
- Only the first line of multi-line messages is included
- Internal paths and class names are filtered out
- Sensitive information should never be included in exception messages

## Integration with Spring GraphQL

The error handler extends `DataFetcherExceptionResolverAdapter` from Spring for GraphQL, which automatically integrates with the Spring GraphQL framework. The handler is registered as a Spring component and will be automatically discovered and used by the GraphQL engine.
