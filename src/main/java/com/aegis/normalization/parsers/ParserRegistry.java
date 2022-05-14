package com.aegis.normalization.parsers;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing event parsers by vendor type
 * Provides lookup functionality to find the appropriate parser for a given vendor
 */
@Component
public class ParserRegistry {
    
    private final Map<String, EventParser> parsers = new ConcurrentHashMap<>();
    private EventParser defaultParser;
    
    @PostConstruct
    public void registerParsers() {
        // Parsers will be registered here as they are implemented
        // Example: parsers.put("aws:cloudtrail", new CloudTrailParser());
        // Example: parsers.put("cisco:asa", new CiscoAsaParser());
    }
    
    /**
     * Gets the parser for the specified vendor type
     * 
     * @param vendorType the vendor type identifier
     * @return the appropriate parser, or default parser if not found
     */
    public EventParser getParser(String vendorType) {
        return parsers.getOrDefault(vendorType, defaultParser);
    }
    
    /**
     * Registers a parser for a specific vendor type
     * 
     * @param vendorType the vendor type identifier
     * @param parser the parser implementation
     */
    public void registerParser(String vendorType, EventParser parser) {
        parsers.put(vendorType, parser);
    }
    
    /**
     * Sets the default parser to use when no specific parser is found
     * 
     * @param parser the default parser
     */
    public void setDefaultParser(EventParser parser) {
        this.defaultParser = parser;
    }
}
