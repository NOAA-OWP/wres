package wres.util;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Christopher Tubbs
 *
 */
public final class Collections
{
    
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
     * Combines two arrays
     * @param left The array to place on the left
     * @param right The array to place on the right
     * @return An array containing the values from the two passed in arrays
     */
    public static <U> U[] combine(U[] left, U[] right) {
        int length = left.length + right.length;

        U[] result = Arrays.copyOf(left, length);

        System.arraycopy(left, 0, result, 0, left.length);

        System.arraycopy(right, 0, result, 0 + left.length, right.length);
        
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
     * @param source The list to filter
     * @param expression The expression used to determine what should be in the list
     * @return A new list containing  all elements that passed through the filter
     */
    public static <U> List<U> where(Collection<U> source, Predicate<U> expression) {
        List<U> filteredValues = null;

        if (source != null)
        {
            filteredValues = source.stream().filter(expression).collect(Collectors.toList());
        }
        return filteredValues;
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

        List<U> remove = where(copy, filter);
        U[] objectsLeft = (U[])Array.newInstance(array[0].getClass(), array.length - remove.size());

        int addedIndex = 0;

        for (final U arrayMember : array) {
            if (!remove.contains(arrayMember)) {
                objectsLeft[addedIndex] = arrayMember;
                addedIndex++;
            }
        }

        return objectsLeft;
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

}
