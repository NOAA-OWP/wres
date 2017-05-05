package util;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import data.EnsembleCache;
import data.FeatureCache;
import data.MeasurementCache;
import data.SourceCache;
import data.VariableCache;

public final class Utilities {
	
	/**
	 * The global format for time is {@value}
	 */
	public final static String TIME_FORMAT = "HH:mm:ss";
	
	/**
	 * The global format for dates is {@value}
	 */
	public final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
	
	/**
	 * Static list of string values that might map to the boolean value 'true'
	 */
	public static final List<String> POSSIBLE_TRUE_VALUES = Arrays.asList("true", "True", "TRUE", "T", "t", "y", "yes", "Yes", "YES", "Y");
	
	/**
	 * Creates a new array without the value at the indicated index
	 * @param array The array to remove the element from
	 * @param index The index of the element to remove
	 * @return A new array without the item at the given index
	 * 
	 * TODO: Modify to take advantage of Arrays.copyOfRange to copy the
	 * array from before and after and combine the two
	 */
	public static <T> T[] removeIndexFromArray(T[] array, int index)
	{
		if (index >= array.length)
		{
			String error = "Cannot remove index %d from an array of length %d.";
			error = String.format(error, index, array.length);
			throw new IndexOutOfBoundsException(error);
		}
		
		T[] copy = (T[])Array.newInstance(array[index].getClass(), array.length - 1);

		for (int i = 0; i < array.length; i++)
		{
			if (i != index)
			{
				if (i < index)
				{
					copy[i] = array[i];
				}
				else
				{
					copy[i - 1] = array[i];
				}
			}
		}
		
		return copy;
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
	
	/**
	 * Combines two arrays
	 * @param left The array to place on the left
	 * @param right The array to place on the right
	 * @return An array containing the values from the two passed in arrays
	 */
	public static <U> U[] combine(U[] left, U[] right) {
	    int length = left.length + right.length;
	    U[] result = (U[])Array.newInstance(left.getClass(), length);
	    
	    int index = 0;
	    
	    for (index = 0; index < left.length; ++index) {
	        result[index] = left[index];
	    }
	    
	    for (index = 0; index < right.length; ++index) {
	        result[index + left.length] = right[index];
	    }
	    
	    return result;
	}
	
	/**
	 * Converts a string array to a comma delimited string containing all
	 * @param strings The strings to combine
	 * @return A comma delimited string containing all passed in strings
	 */
	public static String toString(String[] strings) {
	    return toString(strings, ", ");
	}
	
	/**
	 * Combines all passed in strings delimited by the passed in delimiter
	 * @param strings The strings to combine
	 * @param delimiter A symbol to separate the strings by
	 * @return A delimited string containing all passed in strings
	 */
	public static String toString(String[] strings, String delimiter) {
	       String concat = "";
	        boolean add_delimiter = false;
	        
	        for (String string : strings) {
	            if (add_delimiter) {
	                concat += delimiter;
	            } else {
	                add_delimiter = true;
	            }
	            
	            concat += string;
	        }       	        
	        return concat;
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
	
	/**
	 * Finds the first element in the array that is acceptable by the passed in expression
	 * @param source An array of objects to search through
	 * @param expression An expression that will find a matching element
	 * @return The first found object
	 */
	public static <U> U find(U[] source, Predicate<U> expression) {
	    U found_element = null;
	    
	    for (U element : source) {
	        if (expression.test(element)) {
	            found_element = element;
	            break;
	        }
	    }
	    
	    return found_element;
	}
	
	/**
	 * Determines if a string describes some number
	 * @param possibleNumber A string that might be a number
	 * @return True if the possibleNumber really is a number
	 */
	public static boolean isNumeric(String possibleNumber) {
		return possibleNumber != null && !possibleNumber.isEmpty() && possibleNumber.matches("^[-]?\\d*\\.?\\d+$");
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
	 * Filters a list based on the passed in function
	 * @param source The list to filter
	 * @param expression The expression used to determine what should be in the list
	 * @return A new list containing  all elements that passed through the filter
	 */
	public static <U> List<U> where(Collection<U> source, Predicate<U> expression) {
		return source.stream().filter(expression).collect(Collectors.toList());
	}
	
	/**
	 * Finds an object in the passed in the collection based on the passed in expression
	 * @param source The collection to search through
	 * @param expression The expression used to test elements against
	 * @return The found value. Null if nothing was found
	 */
	public static <U> U find(Collection<U> source, Predicate<U> expression) {
		U val = null;
		List<U> collection = where(source, expression);
		if (collection.size() > 0) {
			val = collection.get(0);
		}
		
		return val;
	}
    
    public static <U> boolean exists(Collection<U> source, Predicate<U> expression) {
        return !where(source, expression).isEmpty();
    }
	
	/**
	 * Determines if the passed in array contains the indicated value
	 * 
	 * Equality is determined by using o.equals rather than ==
	 * @param array The array to search
	 * @param value The value to find
	 * @return Boolean indicating whether or not the indicated value exists within the
	 * indicated array
	 */
	public static <U> boolean contains(U[] array, U value) {
		boolean has_object = false;
		
		for (int index = 0; index < array.length; ++index) {
			if (array[index].equals(value)) {
				has_object = true;
				break;
			}
		}
		
		return has_object;
	}
	
	/**
	 * Gets the text within an xml element
	 * <br>
	 * <b>The stream will attempt to move forward within the source</b>
	 * @param reader The reader for the XML data
	 * @return The trimed text within the xml node. Null is returned if no text is found
	 * @throws XMLStreamException
	 */
	public static String getXMLText(XMLStreamReader reader) throws XMLStreamException {
		String value = null;
		
		if (reader.isStartElement() && (reader.next() == XMLStreamConstants.CHARACTERS)) {
			value = reader.getText().trim();
		}
		
		return value;
	}

	/**
	 * Determines if the xml tag is closed and is one of n possible tag names
	 * 
	 * @param reader The reader for the XML data
	 * @param tagNames A list of names to check against
	 * @return Returns true if the current tag is a closed tag with one of the possible names
	 */
	public static boolean xmlTagClosed(XMLStreamReader reader, List<String> tagNames) {
		if (!reader.isEndElement()) {
			return false;
		}
		boolean hasTagName = false;
		String tag_name = reader.getLocalName();

		// List.contains is not used because we want to ignore casing
		for (String name : tagNames) {
			if (name.equalsIgnoreCase(tag_name)) {
				hasTagName = true;
				break;
			}
		}
		
		return hasTagName;
	}
	
	/**
	 * Searches for and finds the value for the given attribute on the passed in XML node
	 * @param reader The stream containing the XML data
	 * @param attributeName The name of the attribute to search for
	 * @return The value of the attribute on the XML node. Null is returned if the attribute isn't found.
	 */
	public static String getAttributeValue(XMLStreamReader reader, String attributeName) {
		String value = null;
		
		for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex) {
			if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase(attributeName)) {
				value = reader.getAttributeValue(attributeIndex);
			}
		}
		
		return value;
	}
	
	/**
	 * Checks if the tag for the current element is equivalent to the one passed in
	 * @param reader The point in the XML document representing the tag
	 * @param tagName The name of the tag that we are interested in
	 * @return True if, ignoring case, the tag on the current element is the one that we're interested in
	 */
	public static boolean tagIs(XMLStreamReader reader, String tagName) {
		return reader.hasName() && reader.getLocalName().equalsIgnoreCase(tagName);
	}
	
	/**
	 * Loads up all items in every cache
	 * @throws SQLException An error is thrown if values cannot be loaded from the database
	 */
	public static void initializeCaches() throws SQLException {
		MeasurementCache.initialize();
		FeatureCache.initialize();
		EnsembleCache.initialize();
		VariableCache.initialize();
		SourceCache.initialize();
	}
	
	public static String getSystemStats()
	{          
	    Runtime runtime = Runtime.getRuntime();
        
	    final String newline = System.lineSeparator();
	    String stats = newline;
	            
        /* Total number of processors or cores available to the JVM */
        stats += "Available processors (cores):\t" + runtime.availableProcessors() + newline;
        
        /**
         * Function to whittle down and describe the memory in a human readable format
         * (i.e. not represented in raw number of bytes
         */
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
        stats += "Total memory in use:\t\t" + describeMemory.apply(runtime.totalMemory()) + newline;
        stats += newline;
        
        return stats;
	}
}
