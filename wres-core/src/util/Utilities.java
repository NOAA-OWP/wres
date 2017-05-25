package util;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import data.caching.EnsembleCache;
import data.caching.FeatureCache;
import data.caching.MeasurementCache;
import data.caching.SourceCache;
import data.caching.VariableCache;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

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
	public static final List<String> POSSIBLE_TRUE_VALUES = Arrays.asList("true", "True", "TRUE", "T", "t", "y", "yes", "Yes", "YES", "Y", "1");
	
	public static final Map<String, Double> HOUR_CONVERSION = mapTimeToHours();
	
	public static final ProgressMonitor MONITOR = new ProgressMonitor();
	
	private static Map<String, Double> mapTimeToHours()
	{
	    Map<String, Double> mapping = new TreeMap<String, Double>();
        
        mapping.put("second", 1/3600.0);
        mapping.put("hour", 1.0);
        mapping.put("day", 24.0);
        mapping.put("minute", 1/60.0);
	    
	    return mapping;
	}
	
	public static Consumer<Object> defaultOnThreadStartHandler() {
	    return new Consumer() {

            @Override
            public void accept(Object t)
            {
                MONITOR.addStep();
                
            }
	        
	    };
	}
	
	public static Consumer<Object> defaultOnThreadCompleteHandler() {
	    return new Consumer() {

            @Override
            public void accept(Object t)
            {
                MONITOR.UpdateMonitor();
                
            }
	        
	    };
	}
	
	public static final Double secondsToHours(int seconds)
	{
	    return seconds * HOUR_CONVERSION.get("second");
	}
	
	public static final Double secondsToHours(Double seconds)
	{
	    return seconds * HOUR_CONVERSION.get("second");
	}
	
	public static final Double daysToHours(int days)
	{
	    return days * HOUR_CONVERSION.get("day");
	}
	
	public static final Double daysToHours(Double days)
	{
	    return days * HOUR_CONVERSION.get("day");
	}
	
	public static final Double minutesToHours(int minutes)
	{
	    return minutes * HOUR_CONVERSION.get("minute");
	}
	
	public static final Double minutesToHours(Double minutes)
	{
	    return minutes * HOUR_CONVERSION.get("minute");
	}
	
	public static boolean isTrue(String possibleBoolean)
	{
	    return POSSIBLE_TRUE_VALUES.contains(possibleBoolean);
	}
	
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
		
		T[] copy = Arrays.copyOf(array, array.length - 1);

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
	
	public static String extractDay(String date) {
	    String day = null;
	    
	    if (isTimestamp(date)) {
    	    day = extractWord(date, "\\d{1,2}/\\d{1,2}/\\d{4}");
    	    if (day == null) {
    	        day = extractWord(date, "\\d{1,2}-\\d{1,2}-\\d{4}");
    	    }
	    }
	    
	    return day;
	}
	
	public static OffsetDateTime convertStringtoDate(String date, String time, String offset) {
	    return convertStringToDate(date + " " + time, offset);
	}
	
	public static OffsetDateTime convertStringToDate(String datetime, String offset) {
	    OffsetDateTime date = convertStringToDate(datetime);
	    
	    if (date != null && isNumeric(offset)) {
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
	        time = extractWord(date, "\\d{1,2}\\d{2}\\d{2}\\.?\\d*");
	    }
	    
	    return time;
	}
	
	/**
	 * Combines two arrays
	 * @param left The array to place on the left
	 * @param right The array to place on the right
	 * @return An array containing the values from the two passed in arrays
	 */
	public static <U> U[] combine(U[] left, U[] right) {
	    int length = left.length + right.length;

	    U[] result = Arrays.copyOf(left, length);
	    
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
	
	public static <U> U find(Collection<U> source, U comparator, BiPredicate<U, U> expression) {
	    U foundElement = null;
	    for (U element : source) {
	        if (expression.test(element, comparator)) {
	            foundElement = element;
	            break;
	        }
	    }
	    return foundElement;
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
	
	public static <U, V> U getKeyByValue(Map<U, V> mapping, V value)
	{
	    U key = null;
	    for (Entry<U, V> entry : mapping.entrySet())
	    {
	        if (entry.getValue().equals(value)) {
	            key = entry.getKey();
	            break;
	        }
	    }
	    
	    return key;
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
		return reader.isEndElement() && exists(tagNames, (String name) -> {
		   return name.equalsIgnoreCase(reader.getLocalName()); 
		});
	}
	
	public static boolean xmlTagClosed(XMLStreamReader reader, String tagName) {
	    return reader.isEndElement() && reader.getLocalName().equalsIgnoreCase(tagName);
	}
	
	public static void skipToEndTag(XMLStreamReader reader, List<String> tagNames) throws XMLStreamException {
	    while (reader.hasNext() && !xmlTagClosed(reader, tagNames)) {
	        reader.next();
	    }
	}
	
	public static void skipToEndTag(XMLStreamReader reader, String tagName) throws XMLStreamException {
	    while (reader.hasNext() && !xmlTagClosed(reader, tagName)) {
	        reader.next();
	    }
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
	
	public static double getPairedDoubleMean(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
        double mean = 0.0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            mean += pair.getItemOne();
        }
        
        mean /= pairs.size();
        return mean;
	}
	
	public static double getPairedDoubleVectorMean(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
        double mean = 0.0;
        int totalVectorValues = 0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int pairIndex = 0; pairIndex < pair.getItemTwo().length; ++pairIndex)
            {
                mean += pair.getItemTwo()[pairIndex];
                totalVectorValues++;
            }
        }
        
        mean /= totalVectorValues; 
        return mean;
	}
	
	public static double getPairedDoubleStandardDeviation(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
        if (pairs.size() == 1)
        {
            return 0.0;
        }
        
        final double mean = getPairedDoubleMean(pairs);
        int pairCount = 0;
        double STD = 0.0;       
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            STD += Math.pow(pair.getItemOne() - mean, 2);
            pairCount += pair.getItemTwo().length;
        }
        
        STD /= (pairCount - 1);
        
        return Math.sqrt(STD);
	}
	
	public static double getPairedDoubleVectorStandardDeviation(List<PairOfDoubleAndVectorOfDoubles> pairs)
    {
	    if (pairs.size() == 1)
	    {
	        return 0.0;
	    }
	    
        final double mean = getPairedDoubleVectorMean(pairs);
        double STD = 0.0;   
        int pairedValueCount = 0;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            for (int rightIndex = 0; rightIndex < pair.getItemTwo().length; ++rightIndex)
            {
                STD += Math.pow(pair.getItemTwo()[rightIndex] - mean, 2);
                pairedValueCount++;
            }
        }
        
        STD /= (pairedValueCount - 1);
        
        return Math.sqrt(STD);
    }
	
	public static double getCovariance(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
	    if (pairs.size() == 1)
	    {
	        return 0.0;
	    }
	    
	    double pairedDoubleMean = getPairedDoubleMean(pairs);
	    double pairedDoubleVectorMean = getPairedDoubleVectorMean(pairs);
	    double pairCount = 0.0;
	    
	    double covSum = 0.0;
	    
	    for (PairOfDoubleAndVectorOfDoubles pair : pairs)
	    {
	        for (int rightIndex = 0; rightIndex < pair.getItemTwo().length; ++rightIndex)
	        {
	            covSum += (pair.getItemOne() - pairedDoubleMean) * (pair.getItemTwo()[rightIndex] - pairedDoubleVectorMean);
	            pairCount++;
	        }
	    }
	    
	    return covSum / (pairCount - 1);
	}
	
	public static Double getPairedDoubleVectorMax(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
	    Double maximum = null;
	    
	    List<Double> maximumValues = new ArrayList<>(pairs.size());
	    
	    pairs.parallelStream().forEach((PairOfDoubleAndVectorOfDoubles pair) -> {
	        Double max = null;
	        for (int memberIndex = 0; memberIndex < pair.getItemTwo().length; ++memberIndex)
	        {
	            if (max == null || pair.getItemTwo()[memberIndex] > max)
	            {
	                max = pair.getItemTwo()[memberIndex];
	            }
	        }
	        if (max != null)
	        {
	            maximumValues.add(max);
	        }
	    });
	    
	    for (Double value : maximumValues)
	    {
	        if (maximum == null || value > maximum)
	        {
	            maximum = value;
	        }
	    }
	    
	    return maximum;
	}
	
	public static Double getPairedDoubleVectorMin(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
	    Double minimum = null;
        
        List<Double> minimumValues = new ArrayList<>(pairs.size());
        
        pairs.parallelStream().forEach((PairOfDoubleAndVectorOfDoubles pair) -> {
            Double max = null;
            for (int memberIndex = 0; memberIndex < pair.getItemTwo().length; ++memberIndex)
            {
                if (max == null || pair.getItemTwo()[memberIndex] < max)
                {
                    max = pair.getItemTwo()[memberIndex];
                }
            }
            if (max != null)
            {
                minimumValues.add(max);
            }
        });
        
        for (Double value : minimumValues)
        {
            if (minimum == null || value < minimum)
            {
                minimum = value;
            }
        }
	    
	    return minimum;
	}
	
	public static Double getMinimumPairedDouble(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
        Double minimum = null;
        
        for (PairOfDoubleAndVectorOfDoubles pair : pairs)
        {
            if (minimum == null || pair.getItemOne() < minimum)
            {
                minimum = pair.getItemOne();
            }
        }
        
        return minimum;
	}
	
	public static Double getMaximumPairedDouble(List<PairOfDoubleAndVectorOfDoubles> pairs)
	{
	    Double maximum = null;
	    
	    for (PairOfDoubleAndVectorOfDoubles pair : pairs)
	    {
	        if (maximum == null || pair.getItemOne() > maximum)
	        {
	            maximum = pair.getItemOne();
	        }
	    }
	    
	    return maximum;
	}
}
