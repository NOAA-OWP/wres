package wres.util;

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
    public static String convertDateToString( TemporalAccessor datetime )
    {
        return DATE_TIME_FORMATTER.format( datetime );
    }

    /**
     * Hidden constructor.
     */
    private TimeHelper()
    {
    };
}
