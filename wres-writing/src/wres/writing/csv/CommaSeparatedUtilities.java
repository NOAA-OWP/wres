package wres.writing.csv;

import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.DataUtilities;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;

/**
 * Utility class that helps to write Comma Separated Values (CSV).
 *
 * @author James Brown
 */
public class CommaSeparatedUtilities
{

    /**
     * Delimiter for the header.
     */

    public static final String HEADER_DELIMITER = " ";
    private static final String EARLIEST = "EARLIEST";
    private static final String LATEST = "LATEST";
    private static final String VALID = "VALID";
    private static final String ISSUE = "ISSUE";
    private static final String TIME = "TIME";

    /**
     * Returns a default header from the {@link PoolMetadata} to which additional information may be appended.
     *
     * @param sampleMetadata the sample metadata
     * @return default header information
     * @throws NullPointerException if either input is null
     */

    public static StringJoiner getTimeWindowHeaderFromSampleMetadata( PoolMetadata sampleMetadata )
    {
        Objects.requireNonNull( sampleMetadata, "Cannot determine the default CSV header from null metadata." );

        StringJoiner joiner = new StringJoiner( "," );

        String timeScale = "";

        // Set the time-scale string, unless instantaneous
        if ( sampleMetadata.hasTimeScale() )
        {
            TimeScaleOuter s = sampleMetadata.getTimeScale();

            timeScale = timeScale + HEADER_DELIMITER
                        + CommaSeparatedUtilities.getTimeScaleForHeader( s );
        }

        joiner.add( EARLIEST
                    + HEADER_DELIMITER
                    + ISSUE
                    + HEADER_DELIMITER
                    + TIME )
              .add( LATEST
                    + HEADER_DELIMITER
                    + ISSUE
                    + HEADER_DELIMITER
                    + TIME )
              .add( EARLIEST
                    + HEADER_DELIMITER
                    + VALID
                    + HEADER_DELIMITER
                    + TIME )
              .add( LATEST
                    + HEADER_DELIMITER
                    + VALID
                    + HEADER_DELIMITER
                    + TIME )
              .add( EARLIEST
                    + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + TIME
                    + timeScale )
              .add( LATEST
                    + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + TIME
                    + timeScale );

        return joiner;
    }

    /**
     * Creates a timescale string for a header.
     * @param timeScaleOuter the timescale
     * @return the timescale string
     * @throws NullPointerException if the input is null
     */

    public static String getTimeScaleForHeader( TimeScaleOuter timeScaleOuter )
    {
        Objects.requireNonNull( timeScaleOuter );

        String timeScaleString;

        if ( timeScaleOuter.isInstantaneous() )
        {
            timeScaleString = timeScaleOuter.toString();
        }
        else
        {
            StringJoiner innerJoiner = new StringJoiner( HEADER_DELIMITER, "[", "]" );

            // Period present?
            if ( timeScaleOuter.hasPeriod() )
            {

                String period = timeScaleOuter.getFunction()
                                + HEADER_DELIMITER
                                + "OVER PAST"
                                + HEADER_DELIMITER
                                + timeScaleOuter.getPeriod();

                innerJoiner.add( period );
            }
            else
            {
                innerJoiner.add( timeScaleOuter.getFunction().toString() );
            }

            MonthDay startMonthDay = timeScaleOuter.getStartMonthDay();

            if ( Objects.nonNull( startMonthDay ) )
            {
                innerJoiner.add( startMonthDay.toString() );
            }

            MonthDay endMonthDay = timeScaleOuter.getEndMonthDay();

            if ( Objects.nonNull( endMonthDay ) )
            {
                innerJoiner.add( endMonthDay.toString() );
            }

            timeScaleString = innerJoiner.toString();
        }

        return timeScaleString;
    }

    /**
     * Do not construct.
     */
    private CommaSeparatedUtilities()
    {
    }

}
