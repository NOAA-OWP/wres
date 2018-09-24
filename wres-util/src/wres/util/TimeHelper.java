package wres.util;

import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Helper class containing functions used to interpret and modify time data
 * parsed from various data sources (most, in not all, of which are strings)
 */
public final class TimeHelper
{
    /**
     * The global format for dates is {@value}
     */
    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /**
     * The temporal unit that lead numbers in the database represent
     */
    public static final ChronoUnit LEAD_RESOLUTION = ChronoUnit.MINUTES;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern( DATE_FORMAT );
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile( "\\d\\d\\d\\d-\\d\\d-\\d\\d(T| )\\d?\\d:\\d\\d(:\\d\\d\\.?\\d*)?((-|\\+)\\d\\d:?\\d\\d)?Z?" );
    private static final Pattern DATE_PATTERN = Pattern.compile( "\\d\\d\\d\\d(-|/)?\\d\\d(-|/)?\\d\\d" );
    private static final Pattern IMPROPER_OFFSET_PATTERN = Pattern.compile( ".+(-|\\+)\\d\\d\\d\\d$" );
    private static final Pattern PROPER_OFFSET_PATTERN = Pattern.compile( ".+(-|\\+)\\d\\d(:\\d\\d)?$" );
    
    /**
     * Determines if the passed in string represents a date combined with a time
     * @param possibleTimestamp The string that might contain a date and time
     * @return True if the possibleTimestamp really is a timestamp
     */
    public static boolean isTimestamp(String possibleTimestamp)
    {
        return Strings.hasValue( possibleTimestamp ) &&
               TIMESTAMP_PATTERN.matcher( possibleTimestamp ).matches();
    }

    public static boolean isDate(String possibleDate)
    {
        return Strings.hasValue( possibleDate ) && DATE_PATTERN.matcher( possibleDate ).matches();
    }
    
    /**
     * Converts the amount of milliseconds to seconds
     * @param millliseconds The amount of milliseconds to convert
     * @return Integer representing the number of seconds
     */
    public static int secondsFromMilliseconds(long millliseconds)
    {
        return (int)((millliseconds / 1000) % 60);
    }
    
    /**
     * Converts the amount of milliseconds to minutes
     * @param milliseconds The amount of milliseconds to convert
     * @return Integer representing the number of minutes
     */
    public static int minutesFromMilliseconds(long milliseconds)
    {
        return (int)((milliseconds / 60000) % 60);
    }
    
    /**
     * Converts the amount of milliseconds to hours
     * @param milliseconds The amount of milliseconds to convert
     * @return Integer representing the number of hours
     */
    public static int hoursFromMilliseconds(long milliseconds)
    {
        return (int)((milliseconds / 3600000)) % 24;
    }

    /**
     * Converts a passed in time object to the system standard string format
     * @param datetime The date and time to interpret
     * @return The string interpretation of datetime formatted to match {@value #DATE_FORMAT}
     */
    public static String convertDateToString(TemporalAccessor datetime)
    {
        return DATE_TIME_FORMATTER.format( datetime );
    }

    public static <R extends TemporalAccessor> R convertStringToDate(String dateTime, TemporalQuery<R> query)
    {
        return convertStringToDate( dateTime ).query( query );
    }

    /**
     * Attempts to convert a string representation of a date to an actual date
     * object
     *
     * Allows many different types of date and time representations to be used.
     * Formats such as "epoch", "now", "infinity", "1980-01-01", "1980/01/01",
     * "19800101", "1980-01-01 01:00:00", and "1980-01-01T01:00:00-0600" may
     * be used.
     *
     * @param datetime A string representation of a date
     * @return A date and time object representing the described date
     */
    public static TemporalAccessor convertStringToDate(String datetime)
    {
        Objects.requireNonNull( datetime );

        OffsetDateTime date = null;

        datetime = datetime.trim();

        if (isTimestamp(datetime))
        {
            // Allows the use of timestamps of the form "2017-08-08 00:00:00-0600",
            // which can't be parsed; they must be "2017-08-08 00:00:00-06:00"
            if ( IMPROPER_OFFSET_PATTERN.matcher( datetime ).matches())
            {
                datetime = datetime.substring( 0, datetime.length() - 2 ) + ":" + datetime.substring( datetime.length() - 2 );
            }

            // Allows the use of timestamps of the form "2017-08-08 00:00:00",
            // (i.e. without an offset) which can't be parsed. Forces non-offset
            // timestamps into Z time
            if ( !PROPER_OFFSET_PATTERN.matcher( datetime ).matches() && !datetime.endsWith("Z"))
            {
                datetime += "Z";
            }

            // Timestamps such as "2017-08-08 00:00:00Z" can't be parsed,
            // but "2017-08-08T00:00:00Z" can
            if (!datetime.contains("T"))
            {
                datetime = datetime.replace(" ", "T");
            }

            // Timestamps such as "2017-08-08T6:00:00-06:30" cannot be parsed,
            // but "2017-08-08T06:00:00-06:30". Basically, if there aren't
            // two digits after the T, pad it
            if (datetime.charAt( 13 ) != ':' || datetime.matches( "T\\d:" ))
            {
                datetime = datetime.substring( 0, 11 ) + "0" + datetime.substring( 11 );
            }

            date = OffsetDateTime.parse(datetime).withOffsetSameInstant( ZoneOffset.UTC );
        }
        else if (isDate( datetime ))
        {
            // If the date time is missing delimiters between yyyy-mm-dd, add them
            if (datetime.length() == 8)
            {
                datetime = datetime.substring( 0, 4 ) + "-" + datetime.substring( 4, 6 ) + "-" + datetime.substring( 6 );
            }

            // If there were any delimiters of "/" instead of "-", replace them
            datetime = datetime.replaceAll( "/", "-" );

            date = LocalDate.parse( datetime ).atTime( 0, 0 ).atOffset( ZoneOffset.UTC );
        }

        Objects.requireNonNull( date, "The given date ('" +
                                      String.valueOf(datetime) +
                                      "') could not be converted into a date object." );

        return date;
    }

    public static String convertStringDateTimeToDate(String datetime)
    {
        TemporalAccessor actualDateTime = TimeHelper.convertStringToDate( datetime );
        LocalDate actualDate = LocalDate.from( actualDateTime );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
        return actualDate.format(formatter);
    }

    /**
     * Converts the unit and count to the standard resolution of lead times
     *
     * @param unit The current unit of time of the count
     * @param count The number of units
     * @return The count converted into the standard unit for lead times
     */
    public static long unitsToLeadUnits( String unit, Integer count)
    {
        // The units we're expecting should end in an S (HOURS, MINUTES, SECONDS, et)
        // ChronoUnit supports other units, but we shouldn't be using units like "millenia"
        unit = unit.toUpperCase();

        if (!unit.endsWith( "S" ))
        {
            unit += "S";
        }

        return TimeHelper.durationToLongUnits( Duration.of(count.longValue(),
                                                           ChronoUnit.valueOf( unit)), TimeHelper.LEAD_RESOLUTION );
    }

    /**
     * Converts the unit and count to the standard resolution of lead times
     *
     * @param unit The current unit of time of the count
     * @param count The number of units
     * @return The count converted into the standard unit for lead times
     */
    public static long unitsToLeadUnits(String unit, Double count)
    {
        // The units we're expecting should end in an S (HOURS, MINUTES, SECONDS, et)
        // ChronoUnit supports other units, but we shouldn't be using units like "millenia"
        unit = unit.toUpperCase();

        if (!unit.endsWith( "S" ))
        {
            unit += "S";
        }
        return TimeHelper.durationToLongUnits( Duration.of(count.longValue(),
                                                           ChronoUnit.valueOf( unit)), TimeHelper.LEAD_RESOLUTION );
    }

    /**
     * Converts the unit and count to the standard resolution of lead times
     *
     * @param unit The current unit of time of the count
     * @param count The number of units
     * @return The count converted into the standard unit for lead times
     */
    public static long unitsToLeadUnits(String unit, Long count)
    {
        // The units we're expecting should end in an S (HOURS, MINUTES, SECONDS, et)
        // ChronoUnit supports other units, but we shouldn't be using units like "millenia"
        unit = unit.toUpperCase();

        if (!unit.endsWith( "S" ))
        {
            unit += "S";
        }

        return TimeHelper.durationToLongUnits( Duration.of(count,
                                                           ChronoUnit.valueOf( unit)), TimeHelper.LEAD_RESOLUTION );
    }

    /**
     * Retrieves the specified number of time units from the input duration. Accepted units include:
     * 
     * <ol>
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
}
