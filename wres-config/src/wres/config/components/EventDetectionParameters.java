package wres.config.components;


import java.time.Duration;
import java.util.Objects;

import tools.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.serializers.EventDetectionParametersSerializer;

/**
 * Event detection parameters.
 *
 * @param halfLife the half-life or decay term for exponential weighted averaging, which is used to smooth noise
 * @param windowSize the duration over which a moving window is applied for trend detection
 * @param minimumEventDuration the minimum event duration
 * @param startRadius the radius to use when phase shifting events to a local minimum
 * @param combination the method for combining events across data sources
 * @param aggregation the event aggregation method to use when forming an intersection
 */
@RecordBuilder
@JsonSerialize( using = EventDetectionParametersSerializer.class )
public record EventDetectionParameters( Duration halfLife,
                                        Duration windowSize,
                                        Duration minimumEventDuration,
                                        Duration startRadius,
                                        EventDetectionCombination combination,
                                        TimeWindowAggregation aggregation )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EventDetectionParameters.class );

    /**
     * Sets the defaults.
     *
     * @param halfLife the half-life or decay term for exponential weighted averaging, which is used to smooth noise
     * @param windowSize the duration over which a moving window is applied for trend detection
     * @param minimumEventDuration the minimum event duration, which defaults to the half-life, else zero
     * @param startRadius the radius to use when phase shifting events to a local minimum, which defaults to zero
     * @param combination the method for combining events across data sources
     * @param aggregation the event aggregation method to use when forming an intersection
     */
    public EventDetectionParameters
    {
        if ( Objects.isNull( combination ) )
        {
            LOGGER.debug( "Setting the method to combine detected events to {}.", EventDetectionCombination.UNION );
            combination = EventDetectionCombination.UNION;
        }

        LOGGER.debug( "The event detection parameters were set as follows. The half life: {}. The window size: {}. "
                      + "The minimum event duration: {}. The start radius: {}. The combination method: {}. The "
                      + "aggregation method: {}.",
                      halfLife,
                      windowSize,
                      minimumEventDuration,
                      startRadius,
                      combination,
                      aggregation );
    }
}