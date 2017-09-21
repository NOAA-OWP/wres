package wres.util;

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
    
    public static <U> boolean exists(Collection<U> source, Predicate<U> expression) {
        Collection<U> filteredCollection = where(source, expression);
        boolean valueExists = false;

        if (filteredCollection != null)
        {
            valueExists = !filteredCollection.isEmpty();
        }
        return valueExists;
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
    public static <U> boolean contains(final U[] array, final U value) {
        boolean has_object = false;

        for (U arrayValue : array) {
            if (arrayValue.equals(value)) {
                has_object = true;
                break;
            }
        }
        
        return has_object;
    }

    @SuppressWarnings("unchecked")
    public static <U> U[] removeAll (final U[] array, Predicate<U> filter)
    {
        if (array.length == 0)
        {
            return array;
        }
        List<U> copy = Arrays.asList(array);
        copy.removeIf(filter);
        return (U[])copy.toArray();
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
            return String.valueOf( items.toArray()[0] );
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

    public static Double median(Collection<Double> values)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        Double median = null;


        if (values.size() > 0)
        {
            List<Double> sortedValues = new ArrayList<>();

            for ( Double value : values )
            {
                sortedValues.add( value );
            }

            sortedValues.sort( Comparator.naturalOrder() );

            if (sortedValues.size() % 2 == 0)
            {
                int mid = sortedValues.size() / 2;
                median = (sortedValues.get( mid - 1 ) + sortedValues.get(mid)) / 2.0;
            }
            else
            {
                median = sortedValues.get( sortedValues.size() / 2 );
            }
        }

        return median;
    }

    public static Double min(Collection<Double> values)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        Double min = null;

        for (Double value : values)
        {
            if (min == null)
            {
                min = value;
            }
            else
            {
                min = Math.min( min, value );
            }
        }

        return min;
    }

    public static Double mean(Collection<Double> values)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        Double mean = null;

        if (values.size() > 0)
        {
            Double sum = sum( values );
            mean = sum / values.size();
        }

        return mean;
    }

    public static Double sum(Collection<Double> values)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        Double sum = null;

        for (Double value : values)
        {
            if (sum == null)
            {
                sum = value;
            }
            else
            {
                sum += value;
            }
        }

        return sum;
    }

    public static Double max(Collection<Double> values)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        Double max = null;

        for (Double value : values)
        {
            if (max == null)
            {
                max = value;
            }
            else
            {
                max = Math.max( max, value );
            }
        }

        return max;
    }

    public static Double aggregate(final Collection<Double> values, String function)
    {
        if (values.size() == 1)
        {
            for (Double value : values)
            {
                return value;
            }
        }

        function = function.trim().toLowerCase();

        Double aggregatedValue = null;

        switch ( function )
        {
            case "avg":
                aggregatedValue = mean( values );
                break;
            case "median":
                aggregatedValue = median( values );
                break;
            case "max":
                aggregatedValue = max(values);
                break;
            case "min":
                aggregatedValue = min( values );
                break;
            case "sum":
                aggregatedValue = sum(values);
                break;
        }

        return aggregatedValue;
    }

    public static <U extends Comparable<? super U>, V> List<V> getValuesInRange(NavigableMap<U, V> map, U minimum, U maximum)
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
