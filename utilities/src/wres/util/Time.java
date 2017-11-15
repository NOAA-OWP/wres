package wres.util;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Helper class containing functions used to interpret and modify time data
 * parsed from various data sources (most, in not all, of which are strings)
 */
public final class Time
{
    /**
     * The global format for dates is {@value}
     */
    public final static String DATE_FORMAT = "yyyy-MM-dd[ [HH][:mm][:ss]";

    /**
     * Mapping between common date indicators to their conversion multipliers
     * from hours
     *
     * i.e. The mapping of "second" holds the value to multiply a number of
     * hours to get the total number of seconds contained.
     */
    public static final Map<String, Double> HOUR_CONVERSION = mapTimeToHours();
    
    private static Map<String, Double> mapTimeToHours()
    {
        Map<String, Double> mapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        
        mapping.put("second", 1/3600.0);
        mapping.put("hour", 1.0);
        mapping.put("day", 24.0);
        mapping.put("minute", 1/60.0);
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
        return Strings.hasValue( possibleTimestamp ) && (
                possibleTimestamp.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d(T| )\\d\\d:\\d\\d:\\d\\d\\.?\\d*") ||
                Arrays.asList("epoch", "infinity", "-infinity", "now", "today", "tomorrow", "yesterday").contains(possibleTimestamp));
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
     * Converts a passed in time object to the system accepted string format
     * @param datetime The date and time to interpret
     * @return The string interpretation of datetime formatted to match {@value #DATE_FORMAT}
     */
    public static String convertDateToString(OffsetDateTime datetime)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
        return datetime.format(formatter);
    }

    /**
     * Attempts to convert a string representation of a date to an actual date
     * object
     * @param datetime A string representation of a date
     * @return A date and time object representing the described date
     */
    public static OffsetDateTime convertStringToDate(String datetime)
    {
        Objects.requireNonNull( datetime );

        OffsetDateTime date = null;

        datetime = datetime.trim();

        if (datetime.equalsIgnoreCase("epoch"))
        {
            date = OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        }
        else if (datetime.equalsIgnoreCase("infinity"))
        {
            date = OffsetDateTime.MAX;
        }
        else if (datetime.equalsIgnoreCase("-infinity"))
        {
            date = OffsetDateTime.MIN;
        }
        else if (datetime.equalsIgnoreCase("today"))
        {
            date = OffsetDateTime.now();
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        }
        else if (datetime.equalsIgnoreCase("tomorrow"))
        {
            date = OffsetDateTime.now().plusDays(1L);
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        }
        else if (datetime.equalsIgnoreCase("now"))
        {
            date = OffsetDateTime.now(ZoneId.of( "UTC" ));
        }
        else if (datetime.equalsIgnoreCase("yesterday"))
        {
            date = OffsetDateTime.now().minusDays(1L);
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        }
        else if (isTimestamp(datetime))
        {
            if (!datetime.endsWith("Z"))
            {
                datetime += "Z";
            }

            if (!datetime.contains("T"))
            {
                datetime = datetime.replace(" ", "T");
            }

            date = OffsetDateTime.parse(datetime);
        }

        Objects.requireNonNull( date );

        return date;
    }

    public static String convertStringDateTimeToDate(String datetime)
    {
        OffsetDateTime actualDateTime = Time.convertStringToDate( datetime );
        LocalDate actualDate = actualDateTime.toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
        return actualDate.format(formatter);
    }
    
    public static Double unitsToHours(String unit, double count) throws InvalidPropertiesFormatException {

        if (!HOUR_CONVERSION.containsKey(unit))
        {
            throw new IllegalArgumentException(unit + " is not an acceptable unit of time.");
        }

        Double hours;

        try
        {
            hours = HOUR_CONVERSION.get(unit) * count;
        }
        catch (RuntimeException exception)
        {
            throw new InvalidPropertiesFormatException("An error was encountered while trying to multiply the value " +
                                                               String.valueOf(count) +
                                                               " times the factor indicated by " +
                                                               String.valueOf(unit));
        }
        return hours;
    }

    /**
     * Converts a parsed time representation to the format agnostic
     * pattern used across the system
     * @param date The time stamp to normalize
     * @return An updated time string adhering to the defined standard
     */
    public static String normalize(String date)
    {
        Objects.requireNonNull( date );

        OffsetDateTime absoluteDate = Time.convertStringToDate(date);
        absoluteDate = absoluteDate.withOffsetSameInstant( ZoneOffset.UTC );
        return Time.convertDateToString(absoluteDate);
    }

    /**
     * Subtract the specified unit of time from a parsed timestamp
     * @param time The parsed time stamp
     * @param unit The unit that we want to subtract (DAY, HOUR, etc)
     * @param amount The number of the unit to subtract
     * @return A string detailing the updated time stamp
     * @throws InvalidPropertiesFormatException Thrown if the amount to
     * subtract could not be determined from the unit description
     */
    public static String minus(String time, String unit, double amount)
            throws InvalidPropertiesFormatException
    {
        if (time == null)
        {
            throw new IllegalArgumentException( String.valueOf( amount ) +
                                                " " + String.valueOf(unit) +
                                                " could not be removed from " +
                                                "a nonexistent time.");
        }

        // Convert to hours, then convert to seconds
        Double amountToSubtract = Time.unitsToHours( unit, amount ) * 3600;
        OffsetDateTime actualTime = convertStringToDate( time );
        return convertDateToString(
                actualTime.minusSeconds( amountToSubtract.longValue() )
        );
    }
}
