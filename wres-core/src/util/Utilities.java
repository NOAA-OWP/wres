package util;

import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class Utilities {
	
	/**
	 * The global format for time is {@value}
	 */
	private final static String TIME_FORMAT = "HH:mm:ss";
	
	/**
	 * The global format for dates is {@value}
	 */
	private final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
	
	@SuppressWarnings("unchecked")
	/**
	 * Creates a new array without the value at the indicated index
	 * @param array The array to remove the element from
	 * @param index The index of the element to remove
	 * @return A new array without the item at the given index
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
		
		return (T[])copy;
	}
	
	/**
	 * Converts the amount of milliseconds to seconds
	 * @param millliseconds The amount of milliseconds to convert
	 * @return Integer representing the number of seconds
	 * 
	 * TODO: Return a floating point number rather than an integer
	 */
	public static int milliseconds_to_seconds(int millliseconds)
	{
		return (int)(millliseconds / 1000) % 60;
	}
	
	/**
	 * Converts the amount of milliseconds to minutes
	 * @param milliseconds The amount of milliseconds to convert
	 * @return Integer representing the number of minutes
	 * 
	 * TODO: Return a floating point number rather than an integer
	 */
	public static int milliseconds_to_minutes(int milliseconds)
	{
		return (int) ((milliseconds / (1000*60)) % 60);
	}
	
	/**
	 * Converts the amount of milliseconds to hours
	 * @param milliseconds The amount of milliseconds to convert
	 * @return Integer representing the number of hours
	 * 
	 * TODO: Return a floating point number rather than an integer
	 */
	public static int milliseconds_to_hours(int milliseconds)
	{
		return (int) ((milliseconds / (1000*60*60)) % 24);
	}
	
	@Deprecated
	/**
	 * Converts the java.util.Date to the proper date string representation
	 * @param date The date to convert
	 * @return A string representation of the date
	 * @deprecated Remove use of java.util.Date objects
	 */
	public static String convert_date_to_string(Date date)
	{
		DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
		
		return formatter.format(date);
	}
	
	public static String convert_date_to_string(Date date, String time_difference)
	{
		DateFormat formatter = new SimpleDateFormat(DATE_FORMAT + time_difference);
		
		return formatter.format(date);
	}
	
	public static String convert_date_to_string(Calendar cal)
	{
		int offset = cal.getTimeZone().getRawOffset();
		offset = milliseconds_to_hours(offset);
		DateFormat formatter = new SimpleDateFormat(DATE_FORMAT + String.valueOf(offset));
		
		return formatter.format(cal.getTime());
	}

	public static String convert_date_to_string(OffsetDateTime datetime)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);
		return datetime.format(formatter);
	}
	
	public static String convert_time_to_string(LocalTime time)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TIME_FORMAT);
		return time.format(formatter);
	}
		
	public static boolean is_leap_year(int year)
	{
		return (year % 400) == 0 || ( (year % 4) == 0 && (year % 100) != 0);
	}
	
	public static int[] parse_integer_array(final String[] array)
	{
		int[] int_array = new int[array.length];
		
		for (int index = 0; index < array.length; ++index)
		{
			try
			{
				int_array[index] = Integer.parseInt(array[index].replace(".0", ""));
			}
			catch (NumberFormatException error)
			{
				System.err.print("The value '"); 
				System.err.print(array[index]); 
				System.err.print("' at index ");
				System.err.print(index); 
				System.err.println(" could not be converted into an int.");
				throw error;
			}
		}
		
		return int_array;
	}
	
	public static float[] parse_float_array(final String[] array)
	{
		float[] float_array = new float[array.length];
		
		for (int index = 0; index < array.length; ++index)
		{
			try
			{
				float_array[index] = Float.parseFloat(array[index]);
			}
			catch (NumberFormatException error)
			{
				System.err.print("The value '"); 
				System.err.print(array[index]); 
				System.err.print("' at index ");
				System.err.print(index); 
				System.err.println(" could not be converted into a float.");
				throw error;
			}
		}
		
		return float_array;
	}
	
	public static double[] parse_double_array(final String[] array)
	{
		double[] double_array = new double[array.length];
		
		for (int index = 0; index < array.length; ++index)
		{
			try
			{
				double_array[index] = Double.parseDouble(array[index]);
			}
			catch (NumberFormatException error)
			{
				System.err.print("The value '"); 
				System.err.print(array[index]); 
				System.err.print("' at index ");
				System.err.print(index); 
				System.err.println(" could not be converted into an int.");
				throw error;
			}
		}
		
		return double_array;
	}

	public static Float[] combine(Float[] left, Float[] right)
	{
		int length = left.length + right.length;
		Float[] result = new Float[length];
		System.arraycopy(left,  0, result, 0, left.length);
		System.arraycopy(right, 0, result, left.length, right.length);
		return result;
	}
	
	public static Double[] combine(Double[] left, Double[] right)
	{
		int length = left.length + right.length;
		Double[] result = new Double[length];
		System.arraycopy(left,  0, result, 0, left.length);
		System.arraycopy(right, 0, result, left.length, right.length);
		return result;
	}
	
	public static String toString(String[] strings)
	{
		String concat = "";
		boolean add_comma = false;
		
		for (String string : strings)
		{
			if (add_comma)
			{
				concat += ", ";
			}
			else
			{
				add_comma = true;
			}
			
			concat += string;
		}		
		
		return concat;
	}
	
	public static String toString(float[] values)
	{
		String concat = "{";
		boolean add_comma = false;
		
		for (float value : values)
		{
			if (add_comma)
			{
				concat += ", ";
			}
			else
			{
				add_comma = true;
			}
			
			concat += String.valueOf(value);
		}
		
		concat += "}";
		return concat;
	}
	
	public static String toString(Float[] values)
	{
		String concat = "{";
		boolean add_comma = false;
		
		for (float value : values)
		{
			if (add_comma)
			{
				concat += ", ";
			}
			else
			{
				add_comma = true;
			}
			
			concat += String.valueOf(value);
		}
		
		concat += "}";
		return concat;
	}
	
	public static int sum(Integer[] values)
	{
		int total = 0;
		
		for (int index = 0; index < values.length; ++index)
		{
			total += values[index];
		}
		
		return total;
	}
	
	public static float sum(Float[] values)
	{
		float total = 0.0f;
		
		for (int index = 0; index < values.length; ++index)
		{
			total += values[index];
		}
		
		return total;
	}
	
	public static double sum(Double[] values)
	{
		double total = 0.0f;
		
		for (int index = 0; index < values.length; ++index)
		{
			total += values[index];
		}
		
		return total;
	}
	
	public static float mean(Integer[] values)
	{
		return (float)sum(values)/values.length;
	}
	
	public static float mean(Float[] values)
	{
		return sum(values)/values.length;
	}
	
	public static double mean(Double[] values)
	{
		return sum(values)/values.length;
	}
	
	public static String extract_word(String source, String pattern)
	{
		String matched_string = null;
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find())
		{
			matched_string = match.group();
		}
		return matched_string;
	}
	
	public static String[] extract_words(String source, String pattern)
	{
		String[] matches = null;
		
		Pattern regex = Pattern.compile(pattern);
		Matcher match = regex.matcher(source);
		
		if (match.find())
		{
			matches = new String[match.groupCount() + 1];
			for (int match_index = 0; match_index <= match.groupCount(); ++match_index)
			{
				matches[match_index] = match.group(match_index);
			}
		}
				
		return matches;
	}
	
	public static String where(String[] source, Predicate<String> expression)
	{
		String found_element = null;
		
		for (String element : source)
		{
			if (expression.test(element))
			{
				found_element = element;
				break;
			}
		}
		
		return found_element;
	}
	
	public static <U> List<U> where(List<U> source, Predicate<U> expression)
	{
		return source.stream().filter(expression).collect(Collectors.toCollection(()->source.subList(0, 0)));
	}
	
	public static <U> U find(List<U> source, Predicate<U> expression)
	{
		U val = null;
		List<U> collection = where(source, expression);
		if (collection.size() > 0)
		{
			val = collection.get(0);
		}
		
		return val;
	}
}
