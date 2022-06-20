package com.aegis.storage.cold;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Manages Iceberg table schemas for cold tier storage
 */
@Component
public class IcebergSchemaManager {
    private static final Logger logger = LoggerFactory.getLogger(IcebergSchemaManager.class);
    
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
