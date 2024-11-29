package wres.config.yaml.components;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.EventDetectionDeserializer;

/**
 * Used for the detection of discrete events within time-series.
 * @param datasets the datasets to use for event detection
 */
@RecordBuilder
@JsonDeserialize( using = EventDetectionDeserializer.class )
public record EventDetection( @JsonProperty( "dataset" ) Set<EventDetectionDataset> datasets )
{
    /**
     * Creates an instance.
     * @param datasets the datasets to use for event detection
     */
    public EventDetection
    {
        Objects.requireNonNull( datasets, "The event detection dataset cannot be null." );

        if ( datasets.isEmpty() )
        {
            throw new IllegalArgumentException( "Declare at least one dataset for event detection." );
        }
    }
}