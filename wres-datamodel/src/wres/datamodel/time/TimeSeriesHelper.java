package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.sampledata.SampleMetadata;

/**
 * A helper class for time-series.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeSeriesHelper
{

    /**
     * Error message denoting attempt to modify an immutable time-series via an iterator.
     */

    static final String UNSUPPORTED_MODIFICATION = "While attempting to modify an immutable time-series.";

    /**
     * Returns a string representation of the {@link TimeSeriesCollection}.
     * @param <T> the type of time-series
     * @param timeSeries the input time-series
     * @return a string representation
     */
    
    public static <T> String toString( TimeSeriesCollection<T> timeSeries )
    {
        Objects.requireNonNull( timeSeries );
        
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        
        for ( TimeSeries<T> nextSeries : timeSeries.getTimeSeries() )
        {
            for ( Event<T> nextEvent : nextSeries.getEvents() )
            {
                joiner.add( nextEvent.toString() );
            }
        }
    
        return joiner.toString();
    }    
    
    /**
     * Helper method that adjusts the earliest and latest basis times of the {@link TimeWindow} associated with the 
     * input {@link SampleMetadata} when iterating over the atomic time-series by basis time.
     * 
     * @param input the input metadata
     * @param earliestReferenceTime the earliest basis time for the new metadata
     * @param latestReferenceTime the latest basis time for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static SampleMetadata getReferenceTimeAdjustedMetadata( SampleMetadata input,
                                                            Instant earliestReferenceTime,
                                                            Instant latestReferenceTime )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        SampleMetadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = SampleMetadata.of( returnMe,
                                          TimeWindow.of( earliestReferenceTime,
                                                         latestReferenceTime,
                                                         current.getEarliestValidTime(),
                                                         current.getLatestValidTime(),
                                                         current.getEarliestLeadDuration(),
                                                         current.getLatestLeadDuration() ) );
        }
        return returnMe;
    }

    /**
     * Helper method that adjusts the earliest and latest durations of the {@link TimeWindow} associated with the input
     * {@link SampleMetadata} when iterating over the atomic time-series by duration.
     * 
     * @param input the input metadata
     * @param earliestDuration the earliest duration for the new metadata
     * @param latestDuration the latest duration for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static SampleMetadata
            getDurationAdjustedMetadata( SampleMetadata input, Duration earliestDuration, Duration latestDuration )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        SampleMetadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = SampleMetadata.of( returnMe,
                                          TimeWindow.of( current.getEarliestReferenceTime(),
                                                         current.getLatestReferenceTime(),
                                                         current.getEarliestValidTime(),
                                                         current.getLatestValidTime(),
                                                         earliestDuration,
                                                         latestDuration ) );
        }

        return returnMe;
    }

    /**
     * Prevent construction.
     */

    private TimeSeriesHelper()
    {
    }

}
