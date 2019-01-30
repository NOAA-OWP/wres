package wres.util;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.AbstractUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.summary.Sum;

import wres.util.functional.ExceptionalFunction;

/**
 * Helper functions used to operate on collections and arrays
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
     * @return A new collection containing  all elements that passed through the filter
     */
    public static <U> Collection<U> where(Collection<U> source, Predicate<U> expression) {
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

    public static <K, V, W extends Exception> Map<K, List<V>> group(
            Iterable<V> values,
            ExceptionalFunction<V, K, W> mapper
    ) throws W
    {
        Map<K, List<V>> groups = new HashMap<>(  );

        for (V value : values)
        {
            K key = mapper.call( value );

            if (!groups.containsKey( key ))
            {
                groups.put( key, new ArrayList<>() );
            }

            groups.get(key).add(value);
        }

        return groups;
    }

    /**
     * Finds the key of a map based on a passed in function comparing values
     * <p>
     *     The following will find the key of a map with the largest numerical value:
     * </p>
     * <pre>Collections.getKeyByValueFunction(
     *      map,
     *      (newValue, oldValue) -&gt; Math.max(newValue, oldValue) == newValue
     * )
     * </pre>
     * @param map The map to interrogate
     * @param comparisonFunction The function that will find the correct key.
     *                           The first parameter is the value to compare
     *                           against the previous value
     * @param <K> The type of the key
     * @param <V> The type of the value
     * @return The key that is determined to be the match based off of the function
     */
    public static <K, V> K getKeyByValueFunction( Map<K, V> map, BiPredicate<V, V> comparisonFunction)
    {
        K key = null;

        for (Entry<K, V> entry : map.entrySet())
        {
            if (key == null || comparisonFunction.test( entry.getValue(), map.get(key) ))
            {
                key = entry.getKey();
            }
        }

        return key;
    }
    
    /**
     * Finds an object in the passed in the collection based on the passed in expression
     * @param <U> the type of object
     * @param source The collection to search through
     * @param expression The expression used to test elements against
     * @return The found value. Null if nothing was found
     */
    public static <U> U find(Collection<U> source, Predicate<U> expression)
    {
        U val = null;
        for (U value : source)
        {
            if (expression.test( value ))
            {
                val = value;
                break;
            }
        }
        
        return val;
    }

    /**
     * Determines if a value matching the passed in expression exists in the collection
     * @param source The collection of values to look in
     * @param expression a boolean function that will operate on the elements in source
     * @param <U> The type of object in the source collection
     * @return Whether or not an element that fulfills the requirements in the passed
     * in function is within the passed in collection
     */
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

    /**
     * Determines if a value matching the passed in expression exists in the array
     * @param source The array of values to look in
     * @param expression a boolean function that will operate on the elements in source
     * @param <U> The type of object in the source array
     * @return Whether or not an element that fulfills the requirements in the passed
     * in function is within the passed in array
     */
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
     * @param value The value to find
     * @param array The array to search
     * @return Boolean indicating whether or not the indicated value exists within the
     * indicated array
     */
    public static <U> boolean in( final U value, final U[] array )
    {
        boolean hasObject = false;

        for (U arrayValue : array) {
            if (( Objects.isNull(value) && Objects.isNull( arrayValue )) ||
                arrayValue.equals(value))
            {
                hasObject = true;
                break;
            }
        }
        
        return hasObject;
    }

    /**
     * Finds a key matching up with an indicated value
     * @param mapping The mapping between keys and values
     * @param value The value to look for
     * @param <U> The type of the key
     * @param <V> The type of the value
     * @return The first key found with the correct value. If multiple keys have the same value, you're not
     * guaranteed to get the same value each time
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

    /**
     * Copies the items from a collection into a new one, then sorts it
     * @param someCollection A collection of objects
     * @param <U> The type of the object
     * @return A sorted list containing the passed in objects
     */
    public static <U extends Comparable<U>> Collection<U> copyAndSort(Collection<U> someCollection)
    {
        List<U> result = new ArrayList<>(someCollection);
        result.sort(Comparator.naturalOrder());
        return java.util.Collections.unmodifiableList( result );
    }

    /**
     * Translates a listing of values and a specified function to an aggregated
     * value.
     * <p>
     *     <b>Note:</b> NaN is returned if there are missing values
     * </p>
     * @param values The collection of values to aggregate
     * @param function The function to use to aggregate
     * @return The aggregated value
     */
    public static Double aggregate(final Collection<Double> values, String function)
    {
        function = function.trim().toLowerCase();

        double aggregatedValue;

        AbstractUnivariateStatistic operation;

        switch ( function )
        {
            case "mean":
            case "average":
            case "avg":
                operation = new Mean();
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
            case "total":
                operation = new Sum(  );
                break;
            default:
                throw new InvalidParameterException( "The function '" +
                                                     String.valueOf(function) +
                                                     "' is not a valid aggregation function.");
        }

        aggregatedValue = operation.evaluate(
                values.stream()
                      .mapToDouble(
                              value -> {
                                  if (value == null)
                                      return Double.NaN;
                                  else
                                      return value;
                              } ).toArray()
        );

        return aggregatedValue;
    }

    /**
     * Returns each value held between two keys
     * <br>
     * For the map, m:<br>
     * <ul>
     *     <li>1 : "all kinds"</li>
     *     <li>2 : "of",</li>
     *     <li>3 : "s"</li>
     *     <li>4 : "t"</li>
     *     <li>5 : "u"</li>
     *     <li>6 : "f"</li>
     *     <li>7 : "f"</li>
     *     <li>8 : "to"</li>
     *     <li>9 : "look"</li>
     *     <li>10 : "at"</li>
     * </ul>
     * getValuesInRange(m, 2, 7) should return
     * ["s", "t", "u", "f", "f"]
     *
     * @param map A map containing data of interest
     * @param minimum The exclusive minimum key
     * @param maximum The inclusive maximum key
     * @param <U> The type of the key
     * @param <V> The type of the value
     * @return A list containing each value between the minimum and maximum keys
     */
    public static <U extends Comparable<? super U>, V> Collection<V> getValuesInRange(
            NavigableMap<U, V> map,
            U minimum,
            U maximum)
    {
        List<V> values = new ArrayList<>(  );

        map = map.subMap( minimum, false, maximum, true );

        for (Entry<U, V> entry : map.entrySet())
        {
            values.add( entry.getValue() );
        }

        return values;
    }


    public static <V, E extends Exception> String toString(
            V[] values,
            ExceptionalFunction<V, String, E> transformer
    ) throws E
    {
        return Collections.toString(values, transformer, ", ");
    }

    public static <V, E extends Exception> String toString(
            V[] values,
            ExceptionalFunction<V, String, E> transformer,
            final String delimiter
    ) throws E
    {
        StringJoiner joiner = new StringJoiner( delimiter);

        for (V value : values)
        {
            joiner.add(transformer.call( value ));
        }

        return joiner.toString();
    }

    public static <V, E extends Exception> String toString(
            Collection<V> values,
            ExceptionalFunction<V, String, E> transformer
    ) throws E
    {
        return Collections.toString(values, transformer, ", ");
    }

    public static <V, E extends Exception> String toString(
            Collection<V> values,
            ExceptionalFunction<V, String, E> transformer,
            final String delimiter
    ) throws E
    {
        StringJoiner joiner = new StringJoiner( delimiter);

        for (V value : values)
        {
            joiner.add(transformer.call( value ));
        }

        return joiner.toString();
    }
}
