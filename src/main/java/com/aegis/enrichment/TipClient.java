package com.aegis.enrichment;

import com.aegis.domain.ThreatInfo;
import reactor.core.publisher.Mono;

/**
 * TipClient defines the interface for threat intelligence platform (TIP) lookups.
 * Implementations should provide async, non-blocking lookups to external TIP APIs.
 * 
 * This interface supports reactive programming patterns for high-throughput
 * enrichment in the Flink stream processing pipeline.
 */
public interface TipClient {
    
    /**
     * Look up threat intelligence for an IP address.
     * 
     * This method should be non-blocking and return a Mono that completes
     * when the TIP API responds. Implementations should handle:
     * - Rate limiting (429 responses)
     * - Timeouts (default 500ms)
     * - Circuit breaking for failed TIP services
     * 
     * @param ip The IP address to look up
     * @return Mono containing ThreatInfo, or ThreatInfo.SAFE if not found
     */
    Mono<ThreatInfo> lookup(String ip);
    
    /**
     * Look up threat intelligence for a domain.
     * 
     * @param domain The domain to look up
     * @return Mono containing ThreatInfo, or ThreatInfo.SAFE if not found
     */
    Mono<ThreatInfo> lookupDomain(String domain);
    
    /**
     * Look up threat intelligence for a file hash.
     * 
     * @param hash The file hash (MD5, SHA1, or SHA256) to look up
     * @return Mono containing ThreatInfo, or ThreatInfo.SAFE if not found
     */
    Mono<ThreatInfo> lookupHash(String hash);
}
