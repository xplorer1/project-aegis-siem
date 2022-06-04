package com.aegis.ueba;

import com.aegis.domain.UserProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Repository for managing user behavioral profiles in Redis
 */
@Repository
public class UserProfileRepository {
    private static final Logger logger = LoggerFactory.getLogger(UserProfileRepository.class);
    private static final String KEY_PREFIX = "ueba:profile:";
    private static final Duration TTL = Duration.ofDays(90);
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    /**
     * Save a user profile
     * 
     * @param profile The profile to save
     */
    public void save(UserProfile profile) {
        try {
            String key = KEY_PREFIX + profile.getUserId();
            String json = objectMapper.writeValueAsString(profile);
            redisTemplate.opsForValue().set(key, json, TTL);
            logger.debug("Saved profile for user: {}", profile.getUserId());
        } catch (Exception e) {
            logger.error("Failed to save profile for user: {}", profile.getUserId(), e);
            throw new RuntimeException("Profile save failed", e);
        }
    }
    
    /**
     * Find a user profile by user ID
     * 
     * @param userId The user ID
     * @return Optional containing the profile if found
     */
    public Optional<UserProfile> findById(String userId) {
        try {
            String key = KEY_PREFIX + userId;
            String json = redisTemplate.opsForValue().get(key);
            
            if (json == null) {
                return Optional.empty();
            }
            
            UserProfile profile = objectMapper.readValue(json, UserProfile.class);
            return Optional.of(profile);
        } catch (Exception e) {
            logger.error("Failed to find profile for user: {}", userId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Delete a user profile
     * 
     * @param userId The user ID
     */
    public void deleteById(String userId) {
        String key = KEY_PREFIX + userId;
        redisTemplate.delete(key);
        logger.debug("Deleted profile for user: {}", userId);
    }
    
    /**
     * Check if a profile exists
     * 
     * @param userId The user ID
     * @return true if profile exists
     */
    public boolean existsById(String userId) {
        String key = KEY_PREFIX + userId;
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }
}
