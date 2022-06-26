package com.aegis.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web MVC Configuration for AEGIS SIEM
 * 
 * This configuration class registers interceptors and other web-related
 * components for the Spring MVC framework. It is responsible for setting up
 * the request processing pipeline, including security and tenant isolation.
 * 
 * Key Responsibilities:
 * - Register TenantInterceptor for multi-tenant isolation
 * - Configure interceptor path patterns and exclusions
 * - Set interceptor execution order
 * 
 * The TenantInterceptor is applied to all requests except:
 * - Health check endpoints (/actuator/*)
 * - Static resources (/static/*, /css/*, /js/*, /images/*)
 * - Public API documentation (/swagger-ui/*, /v3/api-docs/*)
 * 
 * This ensures that tenant context is properly set for all business logic
 * requests while avoiding unnecessary overhead for infrastructure endpoints.
 * 
 * @see TenantInterceptor
 * @see WebMvcConfigurer
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    
    private static final Logger log = LoggerFactory.getLogger(WebMvcConfig.class);
    
    /**
     * The tenant interceptor to be registered
     */
    private final TenantInterceptor tenantInterceptor;
    
    /**
     * Constructor with dependency injection
     * 
     * @param tenantInterceptor The tenant interceptor instance
     */
    @Autowired
    public WebMvcConfig(TenantInterceptor tenantInterceptor) {
        this.tenantInterceptor = tenantInterceptor;
        log.info("WebMvcConfig initialized with TenantInterceptor");
    }
    
    /**
     * Add interceptors to the registry.
     * 
     * This method registers the TenantInterceptor to run on all requests
     * except for excluded paths (health checks, static resources, etc.).
     * 
     * The interceptor is added with the following configuration:
     * - Applies to all paths by default ("/**")
     * - Excludes infrastructure endpoints that don't need tenant context
     * - Runs early in the interceptor chain (order 0)
     * 
     * Path Patterns:
     * - "/**" matches all paths
     * - "/actuator/**" matches all actuator endpoints
     * - "/static/**" matches all static resource paths
     * 
     * @param registry The interceptor registry
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        log.info("Registering TenantInterceptor");
        
        registry.addInterceptor(tenantInterceptor)
            .addPathPatterns("/**")  // Apply to all paths
            .excludePathPatterns(
                "/actuator/**",      // Health checks and metrics
                "/static/**",        // Static resources
                "/css/**",           // CSS files
                "/js/**",            // JavaScript files
                "/images/**",        // Image files
                "/favicon.ico",      // Favicon
                "/swagger-ui/**",    // Swagger UI
                "/v3/api-docs/**"    // OpenAPI docs
            )
            .order(0);  // Run early in the interceptor chain
        
        log.info("TenantInterceptor registered successfully");
    }
}
