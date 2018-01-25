package wres.util;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.AbstractUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.GeometricMean;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.apache.commons.math3.stat.descriptive.summary.Product;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.commons.math3.stat.descriptive.summary.SumOfLogs;
import org.apache.commons.math3.stat.descriptive.summary.SumOfSquares;

/**
 * @author Christopher Tubbs
 *
 */
public final class Collections
{
    /**
     * Creates a new array without the value at the indicated index
     * 
     * @param <T> the type of object in the array
     * @param array The array to remove the element from
     * @param index The index of the element to remove
     * @return A new array without the item at the given index
     * @throws IndexOutOfBoundsException if the index is out of bounds
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
     * Combines two arrays
     * @param <U> the type of object in the array
     * @param left The array to place on the left
     * @param right The array to place on the right
     * @return An array containing the values from the two passed in arrays
     */
    public static <U> U[] combine(U[] left, U[] right) {
        int length = left.length + right.length;

        U[] result = Arrays.copyOf(left, length);

        System.arraycopy(left, 0, result, 0, left.length);

        System.arraycopy(right, 0, result, left.length, right.length);
        
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
           StringBuilder concat = new StringBuilder();
            boolean add_delimiter = false;
            
            for (String string : strings) {
                if (add_delimiter) {
                    concat.append(delimiter);
                } else {
                    add_delimiter = true;
                }
                
                concat.append(string);
            }                   
            return concat.toString();
    }
    
    /**
     * Finds the first element in the array that is acceptable by the passed in expression
     * 
     * @param <U> the type of object in the source array
     * @param source An array of objects to search through
     * @param expression An expression that will find a matching element
     * @return The first found object
     */
    public static <U> U find(U[] source, Predicate<U> expression)
    {
        U found_element = null;
        
        for (U element : source)
        {
            if (expression.test(element))
            {
                found_element = element;
                break;
            }
        }
        
        return found_element;
    }
    
    /**
     * Filters a list based on the passed in function
     * @param <U> the type of object
     * @param source The list to filter
     * @param expression The expression used to determine what should be in the list
     * @return A new list containing  all elements that passed through the filter
     */
    public static <U> List<U> where(Collection<U> source, Predicate<U> expression) {
        List<U> filteredValues = null;

        if (source != null)
        {
            for (U sourceValue : source)
            {
                if (expression.test( sourceValue ))
                {
                    if (filteredValues == null)
                    {
                        filteredValues = new ArrayList<>(  );
                    }

                    filteredValues.add( sourceValue );
                }
            }
        }
        return filteredValues;
    }
    
    /**
     * Finds an object in the passed in the collection based on the passed in expression
     * @param <U> the type of object
     * @param source The collection to search through
     * @param expression The expression used to test elements against
     * @return The found value. Null if nothing was found
     */
    public static <U> U find(Collection<U> source, Predicate<U> expression) {
        U val = null;
        List<U> collection = where(source, expression);
        if (collection != null && collection.size() > 0) {
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
    
    public static <U> boolean exists(Collection<U> source, Predicate<U> expression)
    {
        Collection<U> filteredCollection = where(source, expression);
        boolean valueExists = false;

        if (filteredCollection != null)
        {
            valueExists = !filteredCollection.isEmpty();
        }
        return valueExists;
    }

    public static <U> boolean exists(U[] source, Predicate<U> expression)
    {
        boolean exists = false;

        for (U value : source)
        {
            if (expression.test( value ))
            {
                exists = true;
                break;
            }
        }

        return exists;
    }
    
    /**
     * Determines if the passed in array contains the indicated value
     * 
     * Equality is determined by using o.equals rather than ==
     * @param <U> the type of object in the array
     * @param array The array to search
     * @param value The value to find
     * @return Boolean indicating whether or not the indicated value exists within the
     * indicated array
     */
    public static <U> boolean contains(final U[] array, final U value)
    {
        boolean has_object = false;

        for (U arrayValue : array) {
            if (arrayValue.equals(value)) {
                has_object = true;
                break;
            }
        }
        
        return has_object;
    }

    /**
     * Finds a key matching up with an indicated value
     * @param mapping The mapping between keys and values
     * @param value The value to look for
     * @param <U> The type of the key
     * @param <V> The type of the value
     * @return The first key found with the correct value. If multiple keys have the same value, you're not
     * guarenteed to get the same value each time
     */
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

    public static <U> String formAnyStatement(Collection<U> items, String typeName)
    {
        if (items.size() == 1)
        {
            String item = String.valueOf( items.toArray()[0] );

            if (typeName.equalsIgnoreCase( "text" ) || typeName.contains( "char" ))
            {
                item = "'" + item + "'";
            }

            return item;
        }

        StringJoiner anyJoiner = new StringJoiner( ",", "ANY('{", "}'::" + typeName + "[])" );
        Set<U> foundKeys = new TreeSet<>(  );

        for (U item : items)
        {
            if (!foundKeys.contains( item ))
            {
                anyJoiner.add( String.valueOf( item ) );
                foundKeys.add( item );
            }
        }

        return anyJoiner.toString();
    }

    /**
     * Translates a listing of values and a specified function to an aggregated
     * value.
     * <p>
     *     <b>Note:</b> If there are missing values or the function is not valid,
     *     NaN is returned
     * </p>
     * @param values The collection of values to aggregate
     * @param function The function to use to aggregate
     * @return The aggregated value
     */
    public static Double aggregate(final Collection<Double> values, String function)
    {
        function = function.trim().toLowerCase();

        Double aggregatedValue = Double.NaN;

        AbstractUnivariateStatistic operation = null;

        switch ( function )
        {
            case "mean":
            case "average":
            case "avg":
                operation = new Mean();
                break;
            case "median":
                operation = new Median(  );
                break;
            case "maximum":
            case "max":
                operation = new Max(  );
                break;
            case "minimum":
            case "min":
                operation = new Min();
                break;
            case "sum":
                operation = new Sum(  );
                break;
            case "sum of logs":
            case "sum_of_logs":
            case "sumoflogs":
                operation = new SumOfLogs(  );
                break;
            case "sum_of_squares":
            case "sum of squares":
            case "sumofsquares":
                operation = new SumOfSquares(  );
                break;
            case "variance":
                operation = new Variance(  );
                break;
            case "second moment":
            case "second_moment":
            case "secondmoment":
                operation = new SecondMoment(  );
                break;
            case "skewness":
                operation = new Skewness(  );
                break;
            case "standard deviation":
            case "standard_deviation":
            case "standarddeviation":
                operation = new StandardDeviation(  );
                break;
            case "geometric_mean":
            case "geometric mean":
            case "geometricmean":
                operation = new GeometricMean(  );
                break;
            case "product":
                operation = new Product();
                break;
            case "kurtosis":
                operation = new Kurtosis(  );
                break;
            default:
                throw new InvalidParameterException( "The function '" +
                                                     String.valueOf(function) +
                                                     "' is not a valid aggregation function.");
        }

        if (operation != null)
        {
            aggregatedValue = operation.evaluate(
                    values.stream()
                          .mapToDouble(
                                  (Double value) -> {
                                      if (value == null)
                                          return Double.NaN;
                                      else
                                          return value.doubleValue();
                                  } ).toArray()
            );
        }

        if (aggregatedValue == null)
        {
            aggregatedValue = Double.NaN;
        }

        return aggregatedValue;
    }

    public static <U extends Comparable<? super U>, V> List<V> getValuesInRange(
            NavigableMap<U, V> map,
            U minimum,
            U maximum)
    {
        List<V> values = new ArrayList<>(  );

        map = map.subMap( minimum, false, maximum, true );

        for (Map.Entry<U, V> entry : map.entrySet())
        {
            values.add( entry.getValue() );
        }

        return values;
    }
}
