package com.aegis.storage.hot;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages OpenSearch index templates for AEGIS events
 */
@Component
public class IndexTemplateManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexTemplateManager.class);
    private static final String TEMPLATE_NAME = "aegis-events-template";
    private static final String INDEX_PATTERN = "aegis-events-*";
    
    @Autowired
    private RestHighLevelClient client;
    
    /**
     * Create index template on startup
     */
    @PostConstruct
    public void createTemplate() {
        try {
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(TEMPLATE_NAME);
            
            // Set index patterns
            request.patterns(java.util.Arrays.asList(INDEX_PATTERN));
            
            // Configure settings
            Settings settings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .put("index.refresh_interval", "5s")
                .put("index.codec", "best_compression")
                .build();
            request.settings(settings);
            
            // Define mappings
            String mappings = buildMappings();
            request.mapping(mappings, XContentType.JSON);
            
            // Set priority
            request.order(1);
            
            // Execute request
            client.indices().putTemplate(request, RequestOptions.DEFAULT);
            
            logger.info("Created index template: {}", TEMPLATE_NAME);
            
        } catch (Exception e) {
            logger.error("Failed to create index template", e);
            // Don't throw - allow application to start even if template creation fails
        }
    }
    
    /**
     * Build JSON mappings for OCSF events
     */
    private String buildMappings() {
        return """
        {
          "properties": {
            "time": {
              "type": "date",
              "format": "epoch_millis"
            },
            "category_name": {
              "type": "keyword"
            },
            "class_name": {
              "type": "keyword"
            },
            "severity": {
              "type": "byte"
            },
            "message": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "actor": {
              "properties": {
                "user": {
                  "properties": {
                    "uid": {
                      "type": "keyword"
                    },
                    "name": {
                      "type": "keyword"
                    }
                  }
                }
              }
            },
            "src_endpoint": {
              "properties": {
                "ip": {
                  "type": "ip"
                },
                "port": {
                  "type": "integer"
                },
                "hostname": {
                  "type": "keyword"
                }
              }
            },
            "dst_endpoint": {
              "properties": {
                "ip": {
                  "type": "ip"
                },
                "port": {
                  "type": "integer"
                },
                "hostname": {
                  "type": "keyword"
                }
              }
            },
            "metadata": {
              "type": "object",
              "enabled": true
            },
            "threat_info": {
              "properties": {
                "reputation_score": {
                  "type": "float"
                },
                "threat_level": {
                  "type": "keyword"
                },
                "categories": {
                  "type": "keyword"
                }
              }
            },
            "ueba_score": {
              "type": "float"
            }
          }
        }
        """;
    }
}
