package com.aegis.storage.cold;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Uploads Parquet files to S3 for cold tier storage
 */
@Component
public class S3Uploader {
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    
    @Value("${aegis.storage.s3.bucket:aegis-cold-tier}")
    private String bucketName;
    
    private final S3Client s3Client;
    
    public S3Uploader() {
        this.s3Client = S3Client.builder().build();
    }
    
    /**
     * Upload Parquet file to S3 with partitioned path
     */
    public String uploadToS3(String localPath, String tenantId) {
        try {
            File file = new File(localPath);
            
            // Generate partitioned S3 key
            LocalDate now = LocalDate.now();
            String s3Key = String.format("events/year=%d/month=%02d/tenant=%s/%s",
                now.getYear(),
                now.getMonthValue(),
                tenantId,
                file.getName()
            );
            
            // Upload to S3
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
            
            s3Client.putObject(request, RequestBody.fromFile(file));
            
            String s3Path = String.format("s3://%s/%s", bucketName, s3Key);
            logger.info("Uploaded file to S3: {}", s3Path);
            
            return s3Path;
            
        } catch (Exception e) {
            logger.error("Failed to upload to S3", e);
            throw new RuntimeException("S3 upload failed", e);
        }
    }
}
