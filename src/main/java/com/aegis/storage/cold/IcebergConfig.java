package com.aegis.storage.cold;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Apache Iceberg cold tier storage
 * Uses S3 as the storage backend
 */
@Configuration
public class IcebergConfig {
    private static final Logger logger = LoggerFactory.getLogger(IcebergConfig.class);
    
    @Value("${aegis.storage.iceberg.warehouse:s3://aegis-cold-tier/warehouse}")
    private String warehousePath;
    
    @Value("${aegis.storage.iceberg.catalog-name:aegis_catalog}")
    private String catalogName;
    
    @Value("${aws.region:us-east-1}")
    private String awsRegion;
    
    /**
     * Create Iceberg catalog
     */
    @Bean
    public Catalog icebergCatalog() {
        try {
            // Configure Hadoop for S3
            org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
            hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            hadoopConf.set("fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
            hadoopConf.set("fs.s3a.endpoint.region", awsRegion);
            
            // Create catalog properties
            Map<String, String> properties = new HashMap<>();
            properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
            properties.put(CatalogProperties.CATALOG_IMPL, HadoopCatalog.class.getName());
            properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            
            // Create Hadoop catalog
            HadoopCatalog catalog = new HadoopCatalog();
            catalog.setConf(hadoopConf);
            catalog.initialize(catalogName, properties);
            
            logger.info("Iceberg catalog initialized: warehouse={}", warehousePath);
            return catalog;
            
        } catch (Exception e) {
            logger.error("Failed to initialize Iceberg catalog", e);
            throw new RuntimeException("Iceberg catalog initialization failed", e);
        }
    }
}
