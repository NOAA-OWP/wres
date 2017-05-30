package wres.util;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Christopher Tubbs
 *
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
    public final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    
    public static final Map<String, Double> HOUR_CONVERSION = mapTimeToHours();
    
    private static Map<String, Double> mapTimeToHours()
    {
        Map<String, Double> mapping = new TreeMap<>();
        
        mapping.put("second", 1/3600.0);
        mapping.put("hour", 1.0);
        mapping.put("day", 24.0);
        mapping.put("minute", 1/60.0);
        
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
                possibleTimestamp.matches("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d-\\d\\d-\\d\\d\\.?\\d*") ||
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
    public static int hoursFromMilliseconds(int milliseconds)
    {
        return (milliseconds / 3600000) % 24;
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
    
    public static String extractDay(String date) {
        String day = null;
        
        if (isTimestamp(date)) {
            day = Strings.extractWord(date, "\\d{1,2}/\\d{1,2}/\\d{4}");
            if (day == null) {
                day = Strings.extractWord(date, "\\d{1,2}-\\d{1,2}-\\d{4}");
            }
        }
        
        return day;
    }
    
    public static OffsetDateTime convertStringtoDate(String date, String time, String offset) {
        return convertStringToDate(date + " " + time, offset);
    }
    
    public static OffsetDateTime convertStringToDate(String datetime, String offset) {
        OffsetDateTime date = convertStringToDate(datetime);
        
        if (date != null && Strings.isNumeric(offset)) {
            date = date.plusHours(Integer.parseInt(offset));
        }
        
        return date;
    }
    
    public static OffsetDateTime convertStringToDate(String datetime) {
        OffsetDateTime date = null;
        
        if (datetime.equalsIgnoreCase("epoch")) {
            date = OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        } else if (datetime.equalsIgnoreCase("infinity")) {
            date = OffsetDateTime.MAX;
        } else if (datetime.equalsIgnoreCase("-infinity")) {
            date = OffsetDateTime.MIN;
        } else if (datetime.equalsIgnoreCase("today")) {
            date = OffsetDateTime.now();
            date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        } else if (datetime.equalsIgnoreCase("tomorrow")) {
            date = OffsetDateTime.now().plusDays(1L);
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        } else if (datetime.equalsIgnoreCase("now")) {
            date = OffsetDateTime.now();
        } else if (datetime.equalsIgnoreCase("yesterday")) {
            date = OffsetDateTime.now().minusDays(1L);
            date = date.minusNanos(date.get(ChronoField.NANO_OF_DAY));
        } else if (isTimestamp(datetime)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
            date = OffsetDateTime.parse(datetime, formatter);
        }
        
        return date;
    }
    
    public static String extractTime(String date) {
        String time = null;
        
        if (isTimestamp(date)) {
            time = Strings.extractWord(date, "\\d{1,2}\\d{2}\\d{2}\\.?\\d*");
        }
        
        return time;
    }
    
    public static Double unitsToHours(String unit, double count)
    {
        return HOUR_CONVERSION.get(unit) * count;
    }
    
    public static Double secondsToHours(int seconds)
    {
        return seconds * HOUR_CONVERSION.get("second");
    }
    
    public static Double secondsToHours(Double seconds)
    {
        return seconds * HOUR_CONVERSION.get("second");
    }
    
    public static Double daysToHours(int days)
    {
        return days * HOUR_CONVERSION.get("day");
    }
    
    public static Double daysToHours(Double days)
    {
        return days * HOUR_CONVERSION.get("day");
    }
    
    public static Double minutesToHours(int minutes)
    {
        return minutes * HOUR_CONVERSION.get("minute");
    }
    
    public static Double minutesToHours(Double minutes)
    {
        return minutes * HOUR_CONVERSION.get("minute");
    }
}
