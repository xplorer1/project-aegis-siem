package com.aegis.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import jakarta.validation.constraints.Pattern;

/**
 * Embeddable class representing a network endpoint in a security event.
 * This class captures information about source or destination endpoints including IP, port, hostname, and MAC address.
 */
public class Endpoint {
    
    @Field(type = FieldType.Ip)
    @JsonProperty("ip")
    @Pattern(regexp = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$", 
             message = "Invalid IP address format")
    private String ip;
    
    @Field(type = FieldType.Integer)
    @JsonProperty("port")
    private Integer port;
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("hostname")
    private String hostname;
    
    @Field(type = FieldType.Keyword)
    @JsonProperty("mac")
    @Pattern(regexp = "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$", 
             message = "Invalid MAC address format")
    private String mac;
    
    /**
     * Default constructor
     */
    public Endpoint() {
    }
    
    /**
     * Constructor with IP
     */
    public Endpoint(String ip) {
        this.ip = ip;
    }
    
    /**
     * Constructor with IP and port
     */
    public Endpoint(String ip, Integer port) {
        this.ip = ip;
        this.port = port;
    }
    
    /**
     * Full constructor
     */
    public Endpoint(String ip, Integer port, String hostname, String mac) {
        this.ip = ip;
        this.port = port;
        this.hostname = hostname;
        this.mac = mac;
    }
    
    // Getters and Setters
    
    public String getIp() {
        return ip;
    }
    
    public void setIp(String ip) {
        this.ip = ip;
    }
    
    public Integer getPort() {
        return port;
    }
    
    public void setPort(Integer port) {
        this.port = port;
    }
    
    public String getHostname() {
        return hostname;
    }
    
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
    
    public String getMac() {
        return mac;
    }
    
    public void setMac(String mac) {
        this.mac = mac;
    }
}
