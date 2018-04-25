package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import wres.config.FeaturePlus;

/**
 * Prototype interface for receiving grid information
 */
public interface Response extends Iterable<List<Response.Series>>
{
    interface Series extends Iterable<Entry>, Comparable<Series>
    {
        FeaturePlus getFeature();
        Instant getIssuedDate();
        Duration getLastLead();
    }

    interface Entry extends Comparable<Entry>, Iterable<Double>
    {
        Duration getLead();
        Instant getValidDate();
        Double[] getMeasurements();
    }

    String getMeasurementUnit();
    String getVariableName();
    Integer getValueCount();
    Duration getLastLead();
}
