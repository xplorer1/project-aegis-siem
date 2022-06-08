package com.aegis.storage.hot;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;

/**
 * Configuration for OpenSearch client
 * Manages connection to OpenSearch cluster for hot tier storage
 */
@Configuration
public class OpenSearchConfig {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConfig.class);
    
    @Value("${aegis.storage.opensearch.hosts:localhost:9200}")
    private String hosts;
    
    @Value("${aegis.storage.opensearch.username:admin}")
    private String username;
    
    @Value("${aegis.storage.opensearch.password:admin}")
    private String password;
    
    @Value("${aegis.storage.opensearch.scheme:https}")
    private String scheme;
    
    private RestHighLevelClient client;
    
    /**
     * Create and configure OpenSearch RestHighLevelClient
     */
    @Bean
    public RestHighLevelClient openSearchClient() {
        try {
            // Parse hosts
            String[] hostArray = hosts.split(",");
            HttpHost[] httpHosts = new HttpHost[hostArray.length];
            
            for (int i = 0; i < hostArray.length; i++) {
                String[] parts = hostArray[i].trim().split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
                httpHosts[i] = new HttpHost(host, port, scheme);
            }
            
            // Configure credentials
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(username, password)
            );
            
            // Build client
            RestClientBuilder builder = RestClient.builder(httpHosts)
                .setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                )
                .setRequestConfigCallback(requestConfigBuilder ->
                    requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(60000)
                );
            
            client = new RestHighLevelClient(builder);
            
            logger.info("OpenSearch client initialized with hosts: {}", hosts);
            return client;
            
        } catch (Exception e) {
            logger.error("Failed to initialize OpenSearch client", e);
            throw new RuntimeException("OpenSearch client initialization failed", e);
        }
    }
    
    /**
     * Clean up OpenSearch client on shutdown
     */
    @PreDestroy
    public void cleanup() {
        if (client != null) {
            try {
                client.close();
                logger.info("OpenSearch client closed");
            } catch (Exception e) {
                logger.error("Error closing OpenSearch client", e);
            }
        }
    }
}
