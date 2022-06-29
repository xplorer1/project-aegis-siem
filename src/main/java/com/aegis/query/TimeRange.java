package com.aegis.query;

/**
 * Represents a time range for query filtering
 */
public class TimeRange {
    private final long earliest;
    private final long latest;

    public TimeRange(long earliest, long latest) {
        this.earliest = earliest;
        this.latest = latest;
    }

    public long getEarliest() {
        return earliest;
    }

    public long getLatest() {
        return latest;
    }
}
