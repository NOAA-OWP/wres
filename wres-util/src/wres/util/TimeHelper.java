package wres.util;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;

/**
 * Helper class containing functions used to interpret and modify time data
 * parsed from various data sources (most, in not all, of which are strings)
 */
public final class TimeHelper
{

    /**
     * The temporal unit that lead numbers in the database represent
     */
    public static final ChronoUnit LEAD_RESOLUTION = ChronoUnit.MINUTES;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    /**
     * Converts a passed in time object to the system standard string format
     * @param datetime The date and time to interpret
     * @return The string interpretation of datetime formatted as ISO instant
     */
    public static String convertDateToString(TemporalAccessor datetime)
    {
        return DATE_TIME_FORMATTER.format( datetime );
    }

    /**
     * Retrieves the specified number of time units from the input duration. Accepted units include:
     * 
     * <ol>
     * <li>{@link ChronoUnit#DAYS}</li>
     * <li>{@link ChronoUnit#HOURS}</li>
     * <li>{@link ChronoUnit#MINUTES}</li>
     * <li>{@link ChronoUnit#SECONDS}</li>
     * <li>{@link ChronoUnit#MILLIS}</li>
     * </ol>
     * 
     * TODO: from JDK9, the interface for obtaining durations as long is improved - consider updating.
     * 
     * @param duration Retrieves the duration
     * @param durationUnits the time units required
     * @return The length of the duration in terms of the project's lead resolution
     * @throws IllegalArgumentException if the durationUnits is not one of the accepted units
     */
    public static long durationToLongUnits( Duration duration, ChronoUnit durationUnits )
    {
        switch ( durationUnits )
        {
            case DAYS:
                return duration.toDays();
            case HOURS:
                return duration.toHours();
            case MINUTES:
                return duration.toMinutes();
            case SECONDS:
                return duration.getSeconds();
            case MILLIS:
                return duration.toMillis();
            default:
                throw new IllegalArgumentException( "The input time units '" + durationUnits
                                                    + "' are not supported "
                                                    + "in this context." );
        }
    }

    public static int durationToLead(Duration duration)
    {
        Long units = TimeHelper.durationToLongUnits( duration, TimeHelper.LEAD_RESOLUTION );
        return units.intValue();
    }
}
