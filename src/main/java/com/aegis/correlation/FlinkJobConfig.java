package com.aegis.correlation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

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
}
