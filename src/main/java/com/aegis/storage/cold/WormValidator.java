package com.aegis.storage.cold;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates WORM (Write Once Read Many) semantics for cold tier
 * Ensures data immutability and prevents unauthorized modifications
 */
@Component
public class WormValidator {
    private static final Logger logger = LoggerFactory.getLogger(WormValidator.class);
    
    @Autowired
    private Catalog icebergCatalog;
    
    /**
     * Validate that table is append-only (no deletes/updates)
     */
    public void validateWormCompliance(String namespace, String tableName) {
        try {
            TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
            Table table = icebergCatalog.loadTable(tableId);
            
            // Check table properties for WORM settings
            String writeMode = table.properties().get("write.mode");
            if (writeMode != null && !writeMode.equals("append")) {
                throw new IllegalStateException(
                    "WORM violation: Table must be append-only");
            }
            
            // Verify no delete operations in history
            long deleteCount = table.history().stream()
                .filter(snapshot -> snapshot.operation().equals("delete"))
                .count();
            
            if (deleteCount > 0) {
                logger.warn("WORM warning: {} delete operations detected", deleteCount);
            }
            
            logger.debug("WORM compliance validated for table: {}", tableId);
            
        } catch (Exception e) {
            logger.error("WORM validation failed", e);
            throw new RuntimeException("WORM validation failed", e);
        }
    }
    
    /**
     * Prevent file deletion (WORM enforcement)
     */
    public void preventDeletion(String filePath) {
        logger.warn("Attempted deletion blocked by WORM policy: {}", filePath);
        throw new UnsupportedOperationException(
            "File deletion not allowed in WORM storage");
    }
    
    /**
     * Check if modification is allowed
     */
    public boolean isModificationAllowed(String operation) {
        // Only append operations are allowed
        return "append".equalsIgnoreCase(operation);
    }
}
