package com.aegis.storage.cold;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Manages Iceberg table schemas for cold tier storage
 */
@Component
public class IcebergSchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(IcebergSchemaManager.class);
    private static final String NAMESPACE = "aegis";
    private static final String TABLE_NAME = "events_cold";
    
    @Autowired
    private Catalog catalog;
    
    /**
     * Create events_cold table on startup
     */
    @PostConstruct
    public void createTable() {
        try {
            TableIdentifier tableId = TableIdentifier.of(NAMESPACE, TABLE_NAME);
            
            // Check if table already exists
            if (catalog.tableExists(tableId)) {
                logger.info("Iceberg table already exists: {}", tableId);
                return;
            }
            
            // Create namespace if it doesn't exist
            try {
                catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of(NAMESPACE));
            } catch (Exception e) {
                // Namespace might already exist
                logger.debug("Namespace creation skipped: {}", e.getMessage());
            }
            
            // Define schema and partition spec
            Schema schema = defineEventSchema();
            PartitionSpec spec = definePartitionSpec();
            
            // Set table properties
            Map<String, String> properties = new HashMap<>();
            properties.put("write.format.default", "parquet");
            properties.put("write.parquet.compression-codec", "snappy");
            properties.put("write.metadata.compression-codec", "gzip");
            properties.put("write.target-file-size-bytes", "536870912"); // 512MB
            
            // Create table
            Table table = catalog.createTable(tableId, schema, spec, properties);
            
            logger.info("Created Iceberg table: {} with {} partitions", 
                tableId, spec.fields().size());
            
        } catch (Exception e) {
            logger.error("Failed to create Iceberg table", e);
            // Don't throw - allow application to start
        }
    }
    
    /**
     * Define OCSF event schema for Iceberg
     */
    public Schema defineEventSchema() {
        return new Schema(
            required(1, "time", Types.TimestampType.withZone()),
            required(2, "tenant_id", Types.StringType.get()),
            required(3, "category_name", Types.StringType.get()),
            required(4, "class_name", Types.StringType.get()),
            required(5, "severity", Types.IntegerType.get()),
            optional(6, "message", Types.StringType.get()),
            
            // Actor fields
            optional(7, "actor_user_uid", Types.StringType.get()),
            optional(8, "actor_user_name", Types.StringType.get()),
            
            // Source endpoint fields
            optional(9, "src_endpoint_ip", Types.StringType.get()),
            optional(10, "src_endpoint_port", Types.IntegerType.get()),
            optional(11, "src_endpoint_hostname", Types.StringType.get()),
            
            // Destination endpoint fields
            optional(12, "dst_endpoint_ip", Types.StringType.get()),
            optional(13, "dst_endpoint_port", Types.IntegerType.get()),
            optional(14, "dst_endpoint_hostname", Types.StringType.get()),
            
            // Enrichment fields
            optional(15, "metadata", Types.StringType.get()),
            optional(16, "threat_reputation_score", Types.FloatType.get()),
            optional(17, "threat_level", Types.StringType.get()),
            optional(18, "ueba_score", Types.FloatType.get())
        );
    }
    
    /**
     * Define partition spec for events table
     * Partitions by year, month, and tenant for efficient querying
     */
    public PartitionSpec definePartitionSpec() {
        return PartitionSpec.builderFor(defineEventSchema())
            .year("time")
            .month("time")
            .identity("tenant_id")
            .build();
    }
}
