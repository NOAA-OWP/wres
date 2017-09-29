package wres.util;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Helper class containing functions used to interpret and modify time data
 * parsed from various data sources (most, in not all, of which are strings)
 */
public final class Time
{
    
    /**
     * The global format for time is {@value}
     */
    public final static String TIME_FORMAT = "HH:mm:ss";
    
    /**
     * The global format for dates is {@value}
     */
    public final static String DATE_FORMAT = "yyyy-MM-dd[ [HH][:mm][:ss][.SSSSSS]";

    /**
     * Mapping between common date indicators to their conversion multipliers
     * from hours
     *
     * i.e. The mapping of "second" holds the value to multiply a number of
     * hours to get the total number of seconds contained.
     */
    public static final Map<String, Double> HOUR_CONVERSION = mapTimeToHours();

    /**
     * A mapping of time zone abbreviations to their offsets in relation to UTC
     * <br><br>
     * The offset is expressed as a Long to accomadate date manipulation functions
     * without type conversions.
     */
    public static final Map<String, Long> TIMEZONE_OFFSET = mapTimeOffsets();
    
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
     * @return A date insensitive mapping between time zone abbreviations and their offsets in relation to UTC
     */
    private static Map<String, Long> mapTimeOffsets()
    {
        Map<String, Long> mapping = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        mapping.put("utc", 0L);
        mapping.put("gmt", 0L);
        mapping.put("EDT", -4L);
        mapping.put("EST", -5L);
        mapping.put("CDT", -5L);
        mapping.put("CST", -6L);
        mapping.put("mdt", -6L);
        mapping.put("mst", -7L);
        mapping.put("pdt", -7L);
        mapping.put("pst", -8L);
        mapping.put("akdt", -8L);
        mapping.put("akst", -9L);
        mapping.put("hadt", -9L);
        mapping.put("hast", -10L);

        return mapping;
    }
    
    /**
     * Determines if the passed in string represents a date combined with a time
     * @param possibleTimestamp The string that might contain a date and time
     * @return True if the possibleTimestamp really is a timestamp
     */
    public static boolean isTimestamp(String possibleTimestamp)
    {
        return possibleTimestamp != null && (
                possibleTimestamp.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.?\\d*") ||
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
     * Converts a parsed date to an actual date and time object, modified by
     * a parsed hourly offset
     * @param datetime The string representation of the original time
     * @param offset The string representation of an hourly offset
     * @return A date and time object for the parsed date modified by a parsed offset
     */
    public static OffsetDateTime convertStringToDate(String datetime, String offset) {
        OffsetDateTime date = convertStringToDate(datetime);
        
        if (date != null && Strings.isNumeric(offset))
        {
            date = date.plusHours(Integer.parseInt(offset));
        }
        
        return date;
    }

    /**
     * Attempts to convert a string representation of a date to an actual date
     * object
     * @param datetime A string representation of a date
     * @return A date and time object representing the described date
     */
    public static OffsetDateTime convertStringToDate(String datetime)
    {
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
            date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        }
        else if (datetime.equalsIgnoreCase("tomorrow"))
        {
            date = OffsetDateTime.now().plusDays(1L);
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        }
        else if (datetime.equalsIgnoreCase("now"))
        {
            date = OffsetDateTime.now();
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
        
        return date;
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
        OffsetDateTime absoluteDate = Time.convertStringToDate(date);
        return Time.convertDateToString(absoluteDate);
    }
    
    /**
     * Gets the number of hours to offset a unit of time depending on the
     * time zone of interest
     * @param timeZone The string representation of a time zone
     * @return A Long representation of offset in UTC
     */
    public static Long zoneOffsetHours(final String timeZone)
    {
    	Long offsetHr = null;
    	
    	 if (TIMEZONE_OFFSET.containsKey(timeZone))
    	 {
    		 offsetHr = TIMEZONE_OFFSET.get(timeZone);
    	 }
    	 
    	 return offsetHr;
    }

    /**
     * Adds the specified unit of time to a parsed time stamp
     * @param time The timestamp to add to
     * @param unit The unit of time to add
     * @param amount The amount of the unit to add
     * @return A string detailing the updated time stamp
     * @throws InvalidPropertiesFormatException Thrown if the amount to
     * add could not be determined from the unit description
     */
    public static String plus( String time, String unit, double amount)
            throws InvalidPropertiesFormatException
    {
        // Convert to hours, then convert to seconds
        Double amountToAdd = Time.unitsToHours( unit, amount ) * 3600;
        OffsetDateTime actualTime = convertStringToDate( time );
        return convertDateToString(
                actualTime.plusSeconds( amountToAdd.longValue() )
        );
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
        // Convert to hours, then convert to seconds
        Double amountToAdd = Time.unitsToHours( unit, amount ) * 3600;
        OffsetDateTime actualTime = convertStringToDate( time );
        return convertDateToString(
                actualTime.minusSeconds( amountToAdd.longValue() )
        );
    }
}
