package com.aegis.ueba;

import ai.onnxruntime.*;
import com.aegis.domain.OcsfEvent;
import com.aegis.domain.UserProfile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * UEBA (User and Entity Behavior Analytics) Scorer
 * Uses ONNX Runtime to score events for anomalous behavior
 */
@Service
public class UebaScorer {
    private static final Logger logger = LoggerFactory.getLogger(UebaScorer.class);
    
    @Value("${aegis.ueba.model-path:models/anomaly-detection.onnx}")
    private String modelPath;
    
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
        // Placeholder - will be implemented in subsequent tasks
        return 0.0;
    }
}
