package com.aegis.ueba;

import com.aegis.domain.Alert;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.UserProfile;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;

/**
 * Flink ProcessFunction for UEBA scoring
 * Scores events for anomalous behavior and generates alerts
 */
@Component
public class UebaScoringFunction extends ProcessFunction<OcsfEvent, OcsfEvent> {
    private static final Logger logger = LoggerFactory.getLogger(UebaScoringFunction.class);
    
    public static final OutputTag<Alert> ALERT_OUTPUT = new OutputTag<Alert>("ueba-alerts"){};
    
    @Autowired
    private UebaScorer scorer;
    
    @Autowired
    private UserProfileRepository profileRepository;
    
    @Override
    public void processElement(OcsfEvent event, Context ctx, Collector<OcsfEvent> out) throws Exception {
        try {
            // Extract user ID from event
            String userId = extractUserId(event);
            if (userId == null) {
                // No user ID, pass through without scoring
                out.collect(event);
                return;
            }
            
            // Get or create user profile
            UserProfile profile = profileRepository.findById(userId)
                .orElseGet(() -> createNewProfile(userId));
            
            // Score the event
            double anomalyScore = scorer.scoreEvent(event, profile);
            
            // Enrich event with UEBA score
            if (event.getMetadata() == null) {
                event.setMetadata(new java.util.HashMap<>());
            }
            event.getMetadata().put("ueba_score", anomalyScore);
            
            // Check if score exceeds threshold
            Alert alert = scorer.checkAnomalyThreshold(event, profile, anomalyScore);
            if (alert != null) {
                ctx.output(ALERT_OUTPUT, alert);
            }
            
            // Update user profile with new event data
            scorer.updateProfile(event, profile);
            
            // Emit enriched event
            out.collect(event);
            
        } catch (Exception e) {
            logger.error("Error processing event for UEBA scoring", e);
            // Pass through event even on error
            out.collect(event);
        }
    }
    
    private String extractUserId(OcsfEvent event) {
        if (event.getActor() != null && event.getActor().getUser() != null) {
            return event.getActor().getUser().getUid();
        }
        return null;
    }
    
    private UserProfile createNewProfile(String userId) {
        UserProfile profile = new UserProfile();
        profile.setUserId(userId);
        profile.setCreatedAt(Instant.now());
        profile.setLastSeen(Instant.now());
        profile.setEventCount(0);
        profile.setAverageDataVolume(0.0);
        profile.setDataVolumeStdDev(1.0);
        profile.setAverageLoginsPerHour(0.0);
        profile.setLoginFrequencyStdDev(1.0);
        profile.setKnownLocations(new ArrayList<>());
        profile.setKnownDevices(new ArrayList<>());
        
        logger.info("Created new user profile for: {}", userId);
        return profile;
    }
}
