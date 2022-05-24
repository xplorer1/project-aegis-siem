package com.aegis.correlation;

import com.aegis.domain.OcsfEvent;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configuration class for Apache Flink stream processing jobs.
 * Configures the StreamExecutionEnvironment with appropriate settings
 * for real-time threat detection and correlation.
 */
@org.springframework.context.annotation.Configuration
public class FlinkJobConfig {

    @Value("${aegis.flink.parallelism:4}")
    private int parallelism;

    @Value("${aegis.flink.max-parallelism:128}")
    private int maxParallelism;

    @Value("${aegis.flink.checkpoint-interval:60000}")
    private long checkpointInterval;

    @Value("${aegis.flink.checkpoint-timeout:600000}")
    private long checkpointTimeout;

    @Value("${aegis.flink.min-pause-between-checkpoints:30000}")
    private long minPauseBetweenCheckpoints;

    @Value("${aegis.flink.checkpoint-dir:file:///tmp/flink-checkpoints}")
    private String checkpointDir;

    @Value("${aegis.flink.restart-attempts:3}")
    private int restartAttempts;

    @Value("${aegis.flink.restart-delay:10000}")
    private long restartDelay;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${aegis.kafka.normalized-events-topic:normalized-events}")
    private String normalizedEventsTopic;

    @Value("${aegis.kafka.consumer-group:aegis-correlation-engine}")
    private String consumerGroup;

    /**
     * Creates and configures the Flink StreamExecutionEnvironment.
     * 
     * Configuration includes:
     * - Parallelism settings for distributed processing
     * - Checkpointing for fault tolerance and exactly-once semantics
     * - Restart strategy for automatic recovery from failures
     * - State backend configuration for stateful operations
     * 
     * @return configured StreamExecutionEnvironment
     */
    @Bean
    public StreamExecutionEnvironment streamExecutionEnvironment() {
        // Create configuration for Flink
        Configuration flinkConfig = new Configuration();
        
        // Disable REST API for embedded mode (can be enabled for standalone clusters)
        flinkConfig.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, false);
        
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        
        // Set runtime mode to STREAMING for continuous processing
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        
        // Configure parallelism
        // Parallelism determines how many parallel instances of each operator run
        // Default is 4, but should be tuned based on available CPU cores and workload
        env.setParallelism(parallelism);
        
        // Set maximum parallelism for future scaling
        // This allows increasing parallelism without losing state
        env.setMaxParallelism(maxParallelism);
        
        // Configure checkpointing for fault tolerance
        // Checkpoints enable exactly-once processing semantics
        env.enableCheckpointing(checkpointInterval);
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        
        // Use exactly-once mode for consistency
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // Set checkpoint timeout - if checkpoint takes longer, it's aborted
        checkpointConfig.setCheckpointTimeout(checkpointTimeout);
        
        // Minimum pause between checkpoints to avoid overwhelming the system
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        
        // Allow only one checkpoint at a time
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        
        // Retain checkpoints when job is cancelled for manual recovery
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // Set checkpoint storage location
        checkpointConfig.setCheckpointStorage(checkpointDir);
        
        // Configure restart strategy for automatic failure recovery
        // Fixed delay restart: retry N times with fixed delay between attempts
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            restartAttempts,
            Time.of(restartDelay, TimeUnit.MILLISECONDS)
        ));
        
        // Configure state backend to RocksDB for large state
        // RocksDB provides efficient disk-based state storage for windowed operations
        // Note: Requires flink-statebackend-rocksdb dependency
        // env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        
        return env;
    }

    /**
     * Gets the configured parallelism level.
     * 
     * @return parallelism level
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Gets the configured maximum parallelism level.
     * 
     * @return maximum parallelism level
     */
    public int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Gets the checkpoint interval in milliseconds.
     * 
     * @return checkpoint interval
     */
    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    /**
     * Gets the checkpoint directory path.
     * 
     * @return checkpoint directory
     */
    public String getCheckpointDir() {
        return checkpointDir;
    }

    /**
     * Creates a KafkaSource for consuming normalized events from Kafka.
     * 
     * The source is configured with:
     * - JSON deserialization for OcsfEvent objects
     * - Consumer group for distributed consumption
     * - Bounded event time watermarks for windowing operations
     * - Automatic offset management with exactly-once semantics
     * 
     * @return configured KafkaSource for OcsfEvent stream
     */
    @Bean
    public KafkaSource<OcsfEvent> kafkaSource() {
        return KafkaSource.<OcsfEvent>builder()
            // Set Kafka bootstrap servers
            .setBootstrapServers(bootstrapServers)
            
            // Set the topic to consume from
            .setTopics(normalizedEventsTopic)
            
            // Set consumer group ID for distributed consumption
            .setGroupId(consumerGroup)
            
            // Start reading from the earliest available offset for new consumer groups
            // This ensures no events are missed when the correlation engine starts
            .setStartingOffsets(OffsetsInitializer.earliest())
            
            // Set deserialization schema for converting Kafka records to OcsfEvent objects
            .setValueOnlyDeserializer(new OcsfEventDeserializationSchema())
            
            // Set bounded out-of-orderness for watermark generation
            // This allows events to arrive up to 5 seconds late before being considered for windows
            .build();
    }

    /**
     * Custom deserialization schema for OcsfEvent objects.
     * Uses Jackson ObjectMapper for JSON deserialization with support for Java 8 time types.
     */
    private static class OcsfEventDeserializationSchema implements DeserializationSchema<OcsfEvent> {
        
        private static final long serialVersionUID = 1L;
        
        private transient ObjectMapper objectMapper;
        
        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            // Initialize ObjectMapper with Java Time module for Instant serialization
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        
        @Override
        public OcsfEvent deserialize(byte[] message) throws IOException {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                objectMapper.registerModule(new JavaTimeModule());
            }
            return objectMapper.readValue(message, OcsfEvent.class);
        }
        
        @Override
        public boolean isEndOfStream(OcsfEvent nextElement) {
            // This is a continuous stream, so never end
            return false;
        }
        
        @Override
        public TypeInformation<OcsfEvent> getProducedType() {
            return TypeInformation.of(OcsfEvent.class);
        }
    }

    /**
     * Creates a WatermarkStrategy for handling event time in the stream.
     * 
     * Watermarks are used to track progress in event time and trigger window computations.
     * This strategy allows events to arrive up to 5 seconds late before being dropped.
     * 
     * @return configured WatermarkStrategy for OcsfEvent stream
     */
    @Bean
    public WatermarkStrategy<OcsfEvent> watermarkStrategy() {
        return WatermarkStrategy
            .<OcsfEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTime().toEpochMilli());
    }
}
