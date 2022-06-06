package com.aegis.ueba;

import ai.onnxruntime.*;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.UserProfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * UEBA (User and Entity Behavior Analytics) Scorer
 * Uses ONNX Runtime to score events for anomalous behavior
 */
@Service
public class UebaScorer {
    private static final Logger logger = LoggerFactory.getLogger(UebaScorer.class);
    private static final int SLIDING_WINDOW_DAYS = 30;
    
    @Value("${aegis.ueba.model-path:models/anomaly-detection.onnx}")
    private String modelPath;
    
    @Autowired
    private UserProfileRepository profileRepository;
    
    private OrtEnvironment env;
    private OrtSession session;
    
    /**
     * Initialize ONNX Runtime environment and load model
     */
    @PostConstruct
    public void init() {
        try {
            env = OrtEnvironment.getEnvironment();
            logger.info("ONNX Runtime environment initialized");
            
            // Load ONNX model
            loadModel();
        } catch (OrtException e) {
            logger.error("Failed to initialize ONNX Runtime", e);
            throw new RuntimeException("ONNX Runtime initialization failed", e);
        }
    }
    
    /**
     * Load ONNX anomaly detection model
     */
    private void loadModel() throws OrtException {
        try {
            if (!Files.exists(Paths.get(modelPath))) {
                logger.warn("ONNX model not found at {}, using mock scoring", modelPath);
                return;
            }
            
            OrtSession.SessionOptions options = new OrtSession.SessionOptions();
            options.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.BASIC_OPT);
            
            session = env.createSession(modelPath, options);
            
            logger.info("ONNX model loaded from {}", modelPath);
            logger.info("Model inputs: {}", session.getInputNames());
            logger.info("Model outputs: {}", session.getOutputNames());
        } catch (Exception e) {
            logger.error("Failed to load ONNX model from {}", modelPath, e);
            throw new OrtException("Model loading failed", e);
        }
    }
    
    /**
     * Clean up ONNX Runtime resources
     */
    @PreDestroy
    public void cleanup() {
        try {
            if (session != null) {
                session.close();
            }
            if (env != null) {
                env.close();
            }
            logger.info("ONNX Runtime resources cleaned up");
        } catch (OrtException e) {
            logger.error("Error cleaning up ONNX Runtime", e);
        }
    }
    
    /**
     * Score an event for anomalous behavior
     * 
     * @param event The event to score
     * @param profile The user's baseline profile
     * @return Anomaly score (0.0 to 1.0)
     */
    public double scoreEvent(OcsfEvent event, UserProfile profile) {
        try {
            // Extract features from event and profile
            float[] features = extractFeatures(event, profile);
            
            // If no model loaded, use simple rule-based scoring
            if (session == null) {
                return simpleRuleBasedScore(features);
            }
            
            // Create feature tensor
            long[] shape = {1, features.length};
            OnnxTensor tensor = OnnxTensor.createTensor(env, FloatBuffer.wrap(features), shape);
            
            // Run ONNX inference
            Map<String, OnnxTensor> inputs = new HashMap<>();
            inputs.put(session.getInputNames().iterator().next(), tensor);
            
            OrtSession.Result result = session.run(inputs);
            
            // Extract anomaly score from output
            float[][] output = (float[][]) result.get(0).getValue();
            double score = output[0][0];
            
            // Clean up
            tensor.close();
            result.close();
            
            logger.debug("Scored event for user {}: {}", profile.getUserId(), score);
            return Math.max(0.0, Math.min(1.0, score)); // Clamp to [0, 1]
            
        } catch (OrtException e) {
            logger.error("ONNX inference failed", e);
            return 0.0;
        }
    }
    
    /**
     * Simple rule-based scoring when ONNX model is not available
     */
    private double simpleRuleBasedScore(float[] features) {
        double score = 0.0;
        
        // High data volume deviation
        if (features[3] > 3.0) score += 0.3;
        
        // High login frequency deviation
        if (features[4] > 3.0) score += 0.2;
        
        // New location
        if (features[5] > 0.5) score += 0.2;
        
        // New device
        if (features[6] > 0.5) score += 0.15;
        
        // Privilege escalation
        if (features[8] > 0.5) score += 0.3;
        
        // Failed login
        if (features[9] > 0.5) score += 0.15;
        
        return Math.min(1.0, score);
    }
    
    /**
     * Extract features from event and user profile
     * 
     * @param event The event
     * @param profile The user profile
     * @return Feature vector
     */
    private float[] extractFeatures(OcsfEvent event, UserProfile profile) {
        // Feature vector: [hour_of_day, day_of_week, data_volume, login_count_deviation, ...]
        float[] features = new float[10];
        
        // Extract temporal features
        features[0] = extractLoginHour(event);
        features[1] = extractDayOfWeek(event);
        
        // Extract data volume features
        features[2] = extractDataVolume(event);
        features[3] = calculateDataVolumeDeviation(event, profile);
        
        // Extract login frequency features
        features[4] = calculateLoginFrequencyDeviation(event, profile);
        
        // Extract location features
        features[5] = isNewLocation(event, profile) ? 1.0f : 0.0f;
        
        // Extract device features
        features[6] = isNewDevice(event, profile) ? 1.0f : 0.0f;
        
        // Extract access pattern features
        features[7] = calculateAccessPatternDeviation(event, profile);
        
        // Extract privilege escalation indicator
        features[8] = hasPrivilegeEscalation(event) ? 1.0f : 0.0f;
        
        // Extract failed login indicator
        features[9] = isFailedLogin(event) ? 1.0f : 0.0f;
        
        return features;
    }
    
    private float extractLoginHour(OcsfEvent event) {
        // Extract hour from timestamp (0-23)
        long timestamp = event.getTime();
        return (timestamp / 3600000) % 24;
    }
    
    private float extractDayOfWeek(OcsfEvent event) {
        // Extract day of week (0-6)
        long timestamp = event.getTime();
        return ((timestamp / 86400000) + 4) % 7; // Epoch was Thursday
    }
    
    private float extractDataVolume(OcsfEvent event) {
        // Extract data volume in MB
        return event.getMetadata() != null && event.getMetadata().containsKey("bytes_transferred")
            ? Float.parseFloat(event.getMetadata().get("bytes_transferred").toString()) / 1_000_000.0f
            : 0.0f;
    }
    
    private float calculateDataVolumeDeviation(OcsfEvent event, UserProfile profile) {
        float currentVolume = extractDataVolume(event);
        double avgVolume = profile.getAverageDataVolume();
        double stdDev = profile.getDataVolumeStdDev();
        
        if (stdDev == 0) return 0.0f;
        
        return (float) Math.abs((currentVolume - avgVolume) / stdDev);
    }
    
    private float calculateLoginFrequencyDeviation(OcsfEvent event, UserProfile profile) {
        int currentHour = (int) extractLoginHour(event);
        double avgLogins = profile.getAverageLoginsPerHour();
        double stdDev = profile.getLoginFrequencyStdDev();
        
        if (stdDev == 0) return 0.0f;
        
        // Simplified - in real implementation would check actual login count
        return (float) Math.abs((1.0 - avgLogins) / stdDev);
    }
    
    private boolean isNewLocation(OcsfEvent event, UserProfile profile) {
        String location = event.getMetadata() != null 
            ? (String) event.getMetadata().get("src_location")
            : null;
        return location != null && !profile.getKnownLocations().contains(location);
    }
    
    private boolean isNewDevice(OcsfEvent event, UserProfile profile) {
        String device = event.getMetadata() != null 
            ? (String) event.getMetadata().get("device_id")
            : null;
        return device != null && !profile.getKnownDevices().contains(device);
    }
    
    private float calculateAccessPatternDeviation(OcsfEvent event, UserProfile profile) {
        // Simplified access pattern deviation
        return 0.0f;
    }
    
    private boolean hasPrivilegeEscalation(OcsfEvent event) {
        return event.getMetadata() != null 
            && Boolean.TRUE.equals(event.getMetadata().get("privilege_escalation"));
    }
    
    private boolean isFailedLogin(OcsfEvent event) {
        return "authentication".equals(event.getCategoryName()) 
            && "failure".equals(event.getMetadata() != null ? event.getMetadata().get("status") : null);
    }
    
    /**
     * Update user profile with new event data
     * Uses sliding window approach to maintain recent behavior baseline
     * 
     * @param event The event
     * @param profile The user profile to update
     */
    public void updateProfile(OcsfEvent event, UserProfile profile) {
        try {
            // Update last seen timestamp
            profile.setLastSeen(Instant.now());
            
            // Update event count
            profile.setEventCount(profile.getEventCount() + 1);
            
            // Update data volume statistics (sliding window)
            updateDataVolumeStats(event, profile);
            
            // Update login frequency statistics
            updateLoginFrequencyStats(event, profile);
            
            // Update known locations
            updateKnownLocations(event, profile);
            
            // Update known devices
            updateKnownDevices(event, profile);
            
            // Save updated profile
            profileRepository.save(profile);
            
            logger.debug("Updated profile for user: {}", profile.getUserId());
        } catch (Exception e) {
            logger.error("Failed to update profile for user: {}", profile.getUserId(), e);
        }
    }
    
    private void updateDataVolumeStats(OcsfEvent event, UserProfile profile) {
        float dataVolume = extractDataVolume(event);
        if (dataVolume > 0) {
            // Simple exponential moving average
            double alpha = 2.0 / (SLIDING_WINDOW_DAYS + 1);
            double oldAvg = profile.getAverageDataVolume();
            double newAvg = alpha * dataVolume + (1 - alpha) * oldAvg;
            profile.setAverageDataVolume(newAvg);
            
            // Update standard deviation (simplified)
            double variance = Math.pow(dataVolume - newAvg, 2);
            double oldStdDev = profile.getDataVolumeStdDev();
            double newStdDev = Math.sqrt(alpha * variance + (1 - alpha) * Math.pow(oldStdDev, 2));
            profile.setDataVolumeStdDev(newStdDev);
        }
    }
    
    private void updateLoginFrequencyStats(OcsfEvent event, UserProfile profile) {
        if ("authentication".equals(event.getCategoryName())) {
            // Update login count
            int hour = (int) extractLoginHour(event);
            
            // Simple exponential moving average
            double alpha = 2.0 / (SLIDING_WINDOW_DAYS + 1);
            double oldAvg = profile.getAverageLoginsPerHour();
            double newAvg = alpha * 1.0 + (1 - alpha) * oldAvg;
            profile.setAverageLoginsPerHour(newAvg);
            
            // Update standard deviation
            double variance = Math.pow(1.0 - newAvg, 2);
            double oldStdDev = profile.getLoginFrequencyStdDev();
            double newStdDev = Math.sqrt(alpha * variance + (1 - alpha) * Math.pow(oldStdDev, 2));
            profile.setLoginFrequencyStdDev(newStdDev);
        }
    }
    
    private void updateKnownLocations(OcsfEvent event, UserProfile profile) {
        String location = event.getMetadata() != null 
            ? (String) event.getMetadata().get("src_location")
            : null;
        if (location != null && !profile.getKnownLocations().contains(location)) {
            profile.getKnownLocations().add(location);
            // Keep only last 50 locations
            if (profile.getKnownLocations().size() > 50) {
                profile.getKnownLocations().remove(0);
            }
        }
    }
    
    private void updateKnownDevices(OcsfEvent event, UserProfile profile) {
        String device = event.getMetadata() != null 
            ? (String) event.getMetadata().get("device_id")
            : null;
        if (device != null && !profile.getKnownDevices().contains(device)) {
            profile.getKnownDevices().add(device);
            // Keep only last 20 devices
            if (profile.getKnownDevices().size() > 20) {
                profile.getKnownDevices().remove(0);
            }
        }
    }
}
