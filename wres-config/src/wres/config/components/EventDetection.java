package wres.config.components;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.EventDetectionDeserializer;

/**
 * Used for the detection of discrete events within time-series.
 *
 * @param datasets the datasets to use for event detection, required
 * @param method the event detection method, optional
 * @param parameters the event detection parameters, optional unless required by a particular method
 */
@RecordBuilder
@JsonDeserialize( using = EventDetectionDeserializer.class )
public record EventDetection( @JsonProperty( "dataset" ) Set<EventDetectionDataset> datasets,
                              @JsonProperty( "method" ) EventDetectionMethod method,
                              EventDetectionParameters parameters )
{
    /**
     * Creates an instance.
     *
     * @param datasets the datasets to use for event detection, required
     * @param method the event detection method, optional
     * @param parameters the event detection parameters, optional unless required by a particular method
     */
    public EventDetection
    {
        Objects.requireNonNull( datasets, "The event detection dataset cannot be null." );

        if ( datasets.isEmpty() )
        {
            throw new IllegalArgumentException( "Declare at least one dataset for event detection." );
        }

        if ( Objects.isNull( method ) )
        {
            method = EventDetectionMethod.DEFAULT;
        }

        // Default parameters
        if ( Objects.isNull( parameters ) )
        {
            parameters = EventDetectionParametersBuilder.builder()
                                                        .build();
        }
    }
}