package com.aegis.graphql;

import graphql.scalars.ExtendedScalars;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

/**
 * Configuration class for GraphQL setup.
 * 
 * Configures custom scalars, error handling, and other GraphQL-specific settings.
 */
@Configuration
public class GraphQLConfig {

    /**
     * Configures runtime wiring for GraphQL schema.
     * Registers custom scalar types like DateTime and JSON.
     * 
     * @return the runtime wiring configurer
     */
    @Bean
    public RuntimeWiringConfigurer runtimeWiringConfigurer() {
        return wiringBuilder -> wiringBuilder
                // Register DateTime scalar for ISO 8601 timestamps
                .scalar(ExtendedScalars.DateTime)
                // Register JSON scalar for arbitrary JSON data
                .scalar(ExtendedScalars.Json)
                // Register Object scalar as alias for JSON
                .scalar(ExtendedScalars.Object);
    }
}
