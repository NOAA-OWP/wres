package wres.util;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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
    public static final String DATE_FORMAT = "yyyy-MM-dd[ [HH][:mm][:ss]";

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern( DATE_FORMAT );
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile( "\\d\\d\\d\\d-\\d\\d-\\d\\d(T| )\\d?\\d:\\d\\d(:\\d\\d\\.?\\d*)?((-|\\+)\\d\\d:?\\d\\d)?Z?" );
    private static final Pattern DATE_PATTERN = Pattern.compile( "\\d\\d\\d\\d(-|/)?\\d\\d(-|/)?\\d\\d" );
    private static final Pattern IMPROPER_OFFSET_PATTERN = Pattern.compile( ".+(-|\\+)\\d\\d\\d\\d$" );
    private static final Pattern PROPER_OFFSET_PATTERN = Pattern.compile( ".+(-|\\+)\\d\\d(:\\d\\d)?$" );
    /**
     * Mapping between common date indicators to their conversion multipliers
     * from hours
     *
     * i.e. The mapping of "second" holds the value to multiply a number of
     * hours to get the total number of seconds contained.
     */
    private static final Map<String, Double> HOUR_CONVERSION = mapTimeToHours();
    
    private static Map<String, Double> mapTimeToHours()
    {
        Map<String, Double> mapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        
        mapping.put("second", 1/3600.0);
        mapping.put("seconds", 1/3600.0);
        mapping.put("hour", 1.0);
        mapping.put("hours", 1.0);
        mapping.put("day", 24.0);
        mapping.put("days", 24.0);
        mapping.put("minute", 1/60.0);
        mapping.put("minutes", 1/60.0);
        mapping.put("s", 1/3600.0);
        mapping.put("hr", 1.0);
        mapping.put("min", 1/60.0);
        
        return mapping;
    }
    
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
     * Takes the string representation of most time formats and forces them
     * into a complete UTC timestamp.
     *
     * We have no control over the format of string dates that the system
     * receives. As a result, they must be standardized.
     *
     * @param datetime A string date of an uncertain format
     * @return A string date in UTC
     */
    public static String standardize(String datetime)
    {
        TemporalAccessor dateObject = TimeHelper.convertStringToDate( datetime );
        return TimeHelper.convertDateToString( dateObject );
    }

    /**
     * Converts the unit and count to the standard unit for lead times
     *
     * The current lead unit is hours. If we seek to change the standard to
     * minutes or seconds, we do it here.
     *
     * TODO: Change the standard to either minutes or seconds
     *
     * @param unit The current unit of time of the count
     * @param count The number of units
     * @return The count converted into the standard unit for lead times
     */
    public static Double unitsToLeadUnits( String unit, double count)
    {
        if (!HOUR_CONVERSION.containsKey(unit))
        {
            throw new IllegalArgumentException(unit + " is not an acceptable unit of time.");
        }

        return HOUR_CONVERSION.get(unit) * count;
    }
}
