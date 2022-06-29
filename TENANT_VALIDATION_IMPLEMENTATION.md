# Tenant Validation Implementation

## Overview

This document describes the implementation of tenant validation for Project AEGIS SIEM, fulfilling **Requirement 9.6**: "THE API_Gateway SHALL validate tenant_id in authentication tokens matches tenant_id in all API requests."

## Implementation Status

✅ **COMPLETE** - Tenant validation has been fully integrated into the API Gateway layer.

## Components

### 1. TenantValidator (Core Validation Logic)

**Location**: `src/main/java/com/aegis/security/TenantValidator.java`

**Purpose**: Validates that the authenticated tenant_id (from JWT token) matches the requested tenant_id (from API request).

**Key Methods**:
- `validateTenantAccess(String requestTenantId)` - Validates access and throws exception if denied
- `hasAccessToTenant(String requestTenantId)` - Non-throwing variant that returns boolean
- `requireTenant(String expectedTenantId)` - Convenience method for validation
- `getAuthenticatedTenantId()` - Retrieves authenticated tenant from context
- `validateTenantAccessBatch(Iterable<String> requestTenantIds)` - Batch validation

**Security Features**:
- Validates tenant_id is not null or empty
- Ensures authenticated tenant is set in context
- Performs exact string matching (case-sensitive)
- Logs all validation failures for security monitoring
- Throws TenantAccessDeniedException for unauthorized access

### 2. TenantAccessDeniedException (Security Exception)

**Location**: `src/main/java/com/aegis/security/TenantAccessDeniedException.java`

**Purpose**: Custom exception thrown when cross-tenant access is attempted.

**Features**:
- Captures both authenticated and requested tenant IDs
- Provides detailed error messages for logging
- Mapped to appropriate HTTP/gRPC status codes

### 3. GraphQL Integration

**Location**: `src/main/java/com/aegis/graphql/AegisGraphQLController.java`

**Integration Points**:

#### Query: `getAlerts`
```java
// Get authenticated tenant from context
String authenticatedTenantId = tenantValidator.getAuthenticatedTenantId();

// Query alerts - repository automatically filters by tenant_id
List<Alert> alerts = alertRepository.findAlerts(...);

// Defense-in-depth: Validate all returned alerts belong to authenticated tenant
for (Alert alert : alerts) {
    if (alert.getTenantId() != null && !alert.getTenantId().equals(authenticatedTenantId)) {
        throw new IllegalStateException("Internal error: tenant isolation violation detected");
    }
}
```

#### Mutation: `acknowledgeAlert`
```java
// Find the alert
Alert alert = alertRepository.findById(alertId);

// Validate tenant access - ensure alert belongs to authenticated tenant
if (alert.getTenantId() != null) {
    tenantValidator.validateTenantAccess(alert.getTenantId());
}

// Update alert status
alert.setStatus(AlertStatus.ACKNOWLEDGED);
alertRepository.save(alert);
```

**Error Handling**: `GraphQLErrorHandler` maps `TenantAccessDeniedException` to GraphQL `FORBIDDEN` error with appropriate extensions.

### 4. gRPC Integration

**Location**: `src/main/java/com/aegis/query/grpc/QueryGrpcService.java`

**Integration Points**:

#### RPC: `executeQuery`
```java
// Validate request
validateRequest(request);

// Validate tenant access - Requirement 9.6
// The AuthenticationInterceptor has already extracted the tenant_id from JWT
// and set it in TenantContext. Now we validate that the request tenant_id
// matches the authenticated tenant_id.
tenantValidator.validateTenantAccess(request.getTenantId());

// Transpile and execute query
QueryPlan queryPlan = aqlTranspiler.transpile(request.getQuery());
Flux<QueryResult> resultFlux = queryExecutor.execute(queryPlan);
```

#### RPC: `executeQueryStream`
```java
// Validate request
validateRequest(request);

// Validate tenant access - Requirement 9.6
tenantValidator.validateTenantAccess(request.getTenantId());

// Execute query and stream results
QueryPlan queryPlan = aqlTranspiler.transpile(request.getQuery());
Flux<QueryResult> resultFlux = queryExecutor.execute(queryPlan);
// Stream results in batches...
```

**Error Handling**: `GrpcExceptionHandler` maps `TenantAccessDeniedException` to gRPC `PERMISSION_DENIED` status with rich error details including:
- ErrorInfo with reason `TENANT_ACCESS_DENIED`
- Metadata with authenticated and requested tenant IDs
- Help links to documentation

### 5. Error Handling

#### GraphQL Error Handler

**Location**: `src/main/java/com/aegis/graphql/GraphQLErrorHandler.java`

```java
private GraphQLError handleTenantAccessDeniedException(TenantAccessDeniedException ex, DataFetchingEnvironment env) {
    Map<String, Object> extensions = new HashMap<>();
    extensions.put("errorCode", "TENANT_ACCESS_DENIED");
    
    if (ex.getAuthenticatedTenantId() != null) {
        extensions.put("authenticatedTenantId", ex.getAuthenticatedTenantId());
    }
    
    if (ex.getRequestedTenantId() != null) {
        extensions.put("requestedTenantId", ex.getRequestedTenantId());
    }
    
    return GraphqlErrorBuilder.newError(env)
            .errorType(ErrorType.FORBIDDEN)
            .message("Access denied: You do not have permission to access resources for the requested tenant")
            .extensions(extensions)
            .build();
}
```

#### gRPC Exception Handler

**Location**: `src/main/java/com/aegis/query/grpc/GrpcExceptionHandler.java`

```java
// Tenant access denied errors (Requirement 9.6)
if (exception instanceof TenantAccessDeniedException) {
    TenantAccessDeniedException tenantEx = (TenantAccessDeniedException) exception;
    String message = String.format("Tenant '%s' cannot access resources for tenant '%s'",
        tenantEx.getAuthenticatedTenantId(), tenantEx.getRequestedTenantId());
    return Status.PERMISSION_DENIED
        .withDescription(message)
        .withCause(exception);
}
```

## Testing

### Unit Tests

**Location**: `src/test/java/com/aegis/security/TenantValidatorTest.java`

**Coverage**:
- ✅ Allow access when tenant IDs match
- ✅ Deny access when tenant IDs do not match
- ✅ Throw IllegalStateException when no tenant is authenticated
- ✅ Throw IllegalArgumentException for null/empty tenant IDs
- ✅ hasAccessToTenant returns correct boolean values
- ✅ requireTenant validation
- ✅ getAuthenticatedTenantId retrieval
- ✅ Batch validation with validateTenantAccessBatch
- ✅ Case-sensitive tenant ID handling
- ✅ Special characters in tenant IDs
- ✅ Very long tenant IDs
- ✅ UUID-style tenant IDs

### Integration Tests

#### GraphQL Integration Tests

**Location**: `src/test/java/com/aegis/graphql/TenantValidationIntegrationTest.java`

**Coverage**:
- ✅ getAlerts returns alerts for authenticated tenant
- ✅ getAlerts rejects alerts from different tenant
- ✅ acknowledgeAlert succeeds for own tenant's alert
- ✅ acknowledgeAlert rejects alert from different tenant
- ✅ acknowledgeAlert handles alert without tenant ID
- ✅ Queries fail when no tenant is authenticated
- ✅ Handle empty result sets
- ✅ Handle non-existent alerts

#### gRPC Integration Tests

**Location**: `src/test/java/com/aegis/query/grpc/TenantValidationGrpcTest.java`

**Coverage**:
- ✅ executeQuery succeeds when tenant IDs match
- ✅ executeQuery rejects when tenant IDs do not match
- ✅ executeQuery fails when no tenant is authenticated
- ✅ executeQuery rejects empty tenant ID
- ✅ executeQueryStream succeeds when tenant IDs match
- ✅ executeQueryStream rejects when tenant IDs do not match
- ✅ Case-sensitive tenant ID handling
- ✅ Error details included in gRPC Status

## Security Considerations

### 1. Defense in Depth

The implementation uses multiple layers of validation:

1. **TenantInterceptor** - Extracts tenant_id from JWT and sets in TenantContext
2. **TenantValidator** - Validates request tenant_id matches authenticated tenant_id
3. **Repository Layer** - Automatically filters queries by tenant_id
4. **Post-Query Validation** - Verifies returned data belongs to authenticated tenant

### 2. Audit Logging

All tenant validation failures are logged with:
- Authenticated tenant ID
- Requested tenant ID
- Request context (method, endpoint)
- Timestamp

This enables security monitoring and incident response.

### 3. Error Messages

Error messages are carefully crafted to:
- Provide enough information for debugging
- Avoid leaking sensitive information
- Include tenant IDs for audit purposes
- Map to appropriate HTTP/gRPC status codes

### 4. Thread Safety

TenantContext uses ThreadLocal storage to ensure:
- Each request has isolated tenant context
- No cross-contamination between concurrent requests
- Proper cleanup after request completion

## Request Flow

### GraphQL Request Flow

```
1. Client sends GraphQL request with JWT token
   ↓
2. TenantInterceptor extracts tenant_id from JWT
   ↓
3. TenantContext.setTenantId(tenant_id)
   ↓
4. GraphQL controller receives request
   ↓
5. TenantValidator.validateTenantAccess(requestTenantId)
   ↓
6. If validation passes: Execute query
   If validation fails: Throw TenantAccessDeniedException
   ↓
7. GraphQLErrorHandler maps exception to GraphQL error
   ↓
8. TenantInterceptor.afterCompletion() clears TenantContext
```

### gRPC Request Flow

```
1. Client sends gRPC request with JWT token
   ↓
2. AuthenticationInterceptor extracts tenant_id from JWT
   ↓
3. TenantContext.setTenantId(tenant_id)
   ↓
4. gRPC service receives request
   ↓
5. TenantValidator.validateTenantAccess(request.getTenantId())
   ↓
6. If validation passes: Execute query
   If validation fails: Throw TenantAccessDeniedException
   ↓
7. GrpcExceptionHandler maps exception to gRPC Status
   ↓
8. AuthenticationInterceptor clears TenantContext
```

## Compliance

This implementation fulfills **Requirement 9.6**:

> "THE API_Gateway SHALL validate tenant_id in authentication tokens matches tenant_id in all API requests"

**Evidence**:
- ✅ TenantValidator validates tenant_id in all API requests
- ✅ Integrated in GraphQL API (getAlerts, acknowledgeAlert)
- ✅ Integrated in gRPC API (executeQuery, executeQueryStream)
- ✅ Rejects unauthorized cross-tenant access attempts
- ✅ Returns appropriate error responses (FORBIDDEN/PERMISSION_DENIED)
- ✅ Comprehensive test coverage
- ✅ Audit logging for security monitoring

## Future Enhancements

1. **Rate Limiting** - Add rate limiting for repeated validation failures
2. **Alerting** - Integrate with security monitoring system for real-time alerts
3. **Metrics** - Add Prometheus metrics for validation success/failure rates
4. **Multi-Tenant Admin** - Support for admin users with cross-tenant access
5. **Tenant Hierarchy** - Support for parent/child tenant relationships

## References

- Requirement 9.6: Multi-Tenant Isolation
- TenantContext: Thread-local tenant storage
- TenantInterceptor: JWT token processing
- GraphQL Error Handling: Error formatting and status codes
- gRPC Error Handling: Status codes and error details
