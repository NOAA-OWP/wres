package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.TimeWindow;

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
     * Helper method that adjusts the earliest and latest basis times of the {@link TimeWindow} associated with the 
     * input {@link SampleMetadata} when iterating over the atomic time-series by basis time.
     * 
     * @param input the input metadata
     * @param earliestTime the earliest basis time for the new metadata
     * @param latestTime the latest basis time for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static SampleMetadata getBasisTimeAdjustedMetadata( SampleMetadata input, Instant earliestTime, Instant latestTime )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        SampleMetadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = SampleMetadata.of( returnMe,
                                                    TimeWindow.of( earliestTime,
                                                                   latestTime,
                                                                   current.getReferenceTime(),
                                                                   current.getEarliestLeadTime(),
                                                                   current.getLatestLeadTime() ) );
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

    static SampleMetadata getDurationAdjustedMetadata( SampleMetadata input, Duration earliestDuration, Duration latestDuration )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        SampleMetadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = SampleMetadata.of( returnMe,
                                                    TimeWindow.of( current.getEarliestTime(),
                                                                   current.getLatestTime(),
                                                                   current.getReferenceTime(),
                                                                   earliestDuration,
                                                                   latestDuration ) );
        }

        return returnMe;
    }

    /**
     * Returns a string representation of the {@link TimeSeries}.
     * @param <T> the type of time-series
     * @param timeSeries the input time-series
     * @return a string representation
     */

    public static <T> String toString( TimeSeries<T> timeSeries )
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        if ( timeSeries.hasMultipleTimeSeries() )
        {
            for ( TimeSeries<T> next : timeSeries.basisTimeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        else
        {
            for ( Event<T> next : timeSeries.timeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        return joiner.toString();
    }

    /**
     * Prevent construction.
     */

    private TimeSeriesHelper()
    {
    }

}
