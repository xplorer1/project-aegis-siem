package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import jakarta.validation.constraints.NotBlank;

/**
 * Embeddable class representing an actor (user, process, or session) in a security event.
 * This class is used within OcsfEvent to capture information about who or what performed an action.
 */
public class Actor {
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("user")
    @NotBlank(message = "User cannot be blank")
    private String user;
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("process")
    private String process;
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("session")
    private String session;
    
    /**
     * Default constructor
     */
    public Actor() {
    }
    
    /**
     * Constructor with user
     */
    public Actor(String user) {
        this.user = user;
    }
    
    /**
     * Full constructor
     */
    public Actor(String user, String process, String session) {
        this.user = user;
        this.process = process;
        this.session = session;
    }
    
    // Getters and Setters
    
    public String getUser() {
        return user;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public String getProcess() {
        return process;
    }
    
    public void setProcess(String process) {
        this.process = process;
    }
    
    public String getSession() {
        return session;
    }
    
    public void setSession(String session) {
        this.session = session;
    }
}
