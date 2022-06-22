package com.aegis.storage.cold;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Writes events to Parquet format for cold tier storage
 */
@Component
public class ParquetWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class);
    
    /**
     * Write events to Parquet file
     */
    public String writeToParquet(List<Map<String, Object>> events, String outputPath) {
        try {
            // Define Avro schema
            Schema schema = defineAvroSchema();
            
            // Configure Parquet writer
            Path path = new Path(outputPath);
            ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(1024 * 1024) // 1MB
                .withRowGroupSize(128 * 1024 * 1024) // 128MB
                .build();
            
            // Write records
            for (Map<String, Object> event : events) {
                GenericRecord record = new GenericData.Record(schema);
                
                record.put("time", event.get("time"));
                record.put("tenant_id", event.get("tenant_id"));
                record.put("category_name", event.get("category_name"));
                record.put("class_name", event.get("class_name"));
                record.put("severity", event.get("severity"));
                record.put("message", event.get("message"));
                record.put("actor_user_uid", event.get("actor_user_uid"));
                record.put("actor_user_name", event.get("actor_user_name"));
                record.put("src_endpoint_ip", event.get("src_endpoint_ip"));
                record.put("src_endpoint_port", event.get("src_endpoint_port"));
                record.put("src_endpoint_hostname", event.get("src_endpoint_hostname"));
                record.put("dst_endpoint_ip", event.get("dst_endpoint_ip"));
                record.put("dst_endpoint_port", event.get("dst_endpoint_port"));
                record.put("dst_endpoint_hostname", event.get("dst_endpoint_hostname"));
                record.put("metadata", event.get("metadata"));
                record.put("threat_reputation_score", event.get("threat_reputation_score"));
                record.put("threat_level", event.get("threat_level"));
                record.put("ueba_score", event.get("ueba_score"));
                
                writer.write(record);
            }
            
            writer.close();
            
            logger.info("Wrote {} events to Parquet file: {}", events.size(), outputPath);
            return outputPath;
            
        } catch (IOException e) {
            logger.error("Failed to write Parquet file", e);
            throw new RuntimeException("Parquet write failed", e);
        }
    }
    
    /**
     * Define Avro schema for OCSF events
     */
    private Schema defineAvroSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "OcsfEvent",
              "namespace": "com.aegis.storage",
              "fields": [
                {"name": "time", "type": ["null", "long"], "default": null},
                {"name": "tenant_id", "type": ["null", "string"], "default": null},
                {"name": "category_name", "type": ["null", "string"], "default": null},
                {"name": "class_name", "type": ["null", "string"], "default": null},
                {"name": "severity", "type": ["null", "int"], "default": null},
                {"name": "message", "type": ["null", "string"], "default": null},
                {"name": "actor_user_uid", "type": ["null", "string"], "default": null},
                {"name": "actor_user_name", "type": ["null", "string"], "default": null},
                {"name": "src_endpoint_ip", "type": ["null", "string"], "default": null},
                {"name": "src_endpoint_port", "type": ["null", "int"], "default": null},
                {"name": "src_endpoint_hostname", "type": ["null", "string"], "default": null},
                {"name": "dst_endpoint_ip", "type": ["null", "string"], "default": null},
                {"name": "dst_endpoint_port", "type": ["null", "int"], "default": null},
                {"name": "dst_endpoint_hostname", "type": ["null", "string"], "default": null},
                {"name": "metadata", "type": ["null", "string"], "default": null},
                {"name": "threat_reputation_score", "type": ["null", "float"], "default": null},
                {"name": "threat_level", "type": ["null", "string"], "default": null},
                {"name": "ueba_score", "type": ["null", "float"], "default": null}
              ]
            }
            """;
        
        return new Schema.Parser().parse(schemaJson);
    }
}
