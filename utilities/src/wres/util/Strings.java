package wres.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Strings {

	private final static int TRUNCATE_SIZE = 1000;

    private Strings(){}
	
	/**
	 * Static list of string values that might map to the boolean value 'true'
	 */
	public static final List<String> POSSIBLE_TRUE_VALUES = Arrays.asList("true", "True", "TRUE", "T", "t", "y", "yes", "Yes", "YES", "Y", "1");
	
	public static boolean isTrue(String possibleBoolean)
	{
	    return POSSIBLE_TRUE_VALUES.contains(possibleBoolean);
	}
	
	/**
	 * Extracts the first grouping of characters in the source string that matches the pattern
	 * @param source The string to extract the word from
	 * @param pattern The pattern to match
	 * @return The first substring to match the pattern
	 */
	public static String extractWord(String source, String pattern) {
		String matched_string = null;
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find()) {
			matched_string = match.group();
		}
		return matched_string;
	}

	public static String truncate(String message)
	{
		return truncate(message, TRUNCATE_SIZE);
	}

	public static String truncate(String message, int length)
	{
		String truncatedMessage = message;
		if (message.length() > length - 3 && length > 3)
		{
			truncatedMessage = message.substring(0, length - 3) + "...";
		}
		return truncatedMessage;
	}
	
	/**
	 * Finds every substring that in the source that matches the pattern
	 * @param source The string to extract the words from
	 * @param pattern The pattern to match
	 * @return A string array containing all matched substrings
	 */
	public static String[] extractWords(String source, String pattern) {
		String[] matches = null;
		
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find()) {
			matches = new String[match.groupCount() + 1];
			for (int match_index = 0; match_index <= match.groupCount(); ++match_index) {
				matches[match_index] = match.group(match_index);
			}
		}
				
		return matches;
	}

	public static boolean contains(String full, String pattern)
	{
		Pattern regex = Pattern.compile(pattern);
		return regex.matcher(full).find();
	}
	
	/**
	 * Determines if a string describes some number
	 * @param possibleNumber A string that might be a number
	 * @return True if the possibleNumber really is a number
	 */
	public static boolean isNumeric(String possibleNumber) {
		return possibleNumber != null && !possibleNumber.isEmpty() && possibleNumber.matches("^[-]?\\d*\\.?\\d+$");
	}
	
	public static String getSystemStats()
	{          
	    Runtime runtime = Runtime.getRuntime();
        
	    final String newline = System.lineSeparator();
	    String stats = newline;
	            
        /* Total number of processors or cores available to the JVM */
        stats += "Available processors (cores):\t" + runtime.availableProcessors() + newline;

        Function<Long, String> describeMemory = (Long memory) -> {
            String memoryUnit = " bytes";
            Double floatingMemory = memory.doubleValue();
            
            // Convert to KB if necessary
            if (floatingMemory > 1000)
            {
                floatingMemory = floatingMemory / 1000.0;
                memoryUnit = " KB";
            }
            
            // Convert to MB if Necessary
            if (floatingMemory > 1000)
            {
                floatingMemory = floatingMemory / 1000.0;
                memoryUnit = " MB";
            }
            
            // Convert to GB if necessary
            if (floatingMemory > 1000)
            {
                floatingMemory = floatingMemory / 1000.0;
                memoryUnit = " GB";
            }
            
            // Convert to TB if necessary
            if (floatingMemory > 1000)
            {
                floatingMemory = floatingMemory / 1000.0;
                memoryUnit = " TB";
            }
            
            return floatingMemory + memoryUnit;
        };
        
        // Theoretical amount of free memory
        stats += "Free memory:\t\t\t" + describeMemory.apply(runtime.freeMemory()) + newline;
        
        /* This will return Long.MAX_VALUE if there is no preset limit */
        long maxMemory = runtime.maxMemory();
        
        /* Maximum amount of memory the JVM will attempt to use */
        stats += "Maximum available memory:\t" + (maxMemory == Long.MAX_VALUE ? "no limit" : describeMemory.apply(maxMemory)) + newline;
        
        /* Total memory currently in use by the JVM */
        stats += "Total memory in use:\t\t" + describeMemory.apply(runtime.totalMemory() - runtime.freeMemory()) + newline;
        stats += newline;
        
        return stats;
	}
	
	public static String getStackTrace(Exception error)
	{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        error.printStackTrace(ps);
        ps.close();
        return baos.toString();
	}

	public static boolean isOneOf(String possible, String... options)
	{
		boolean isOne = false;

		for (String option : options)
		{
			if (possible.equalsIgnoreCase(option))
			{
				isOne = true;
				break;
			}
		}

		return isOne;
	}
}
