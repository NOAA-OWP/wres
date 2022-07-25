package wres.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;

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
    public static <T> T[] removeIndexFromArray( T[] array, int index )
    {
        if ( index >= array.length )
        {
            String error = "Cannot remove index %d from an array of length %d.";
            error = String.format( error, index, array.length );
            throw new IndexOutOfBoundsException( error );
        }

        T[] copy = Arrays.copyOf( array, array.length - 1 );

        for ( int i = 0; i < array.length; i++ )
        {
            if ( i != index )
            {
                if ( i < index )
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
    public static String toString( String[] strings )
    {
        return toString( strings, ", " );
    }

    /**
     * Combines all passed in strings delimited by the passed in delimiter
     * @param strings The strings to combine
     * @param delimiter A symbol to separate the strings by
     * @return A delimited string containing all passed in strings
     */
    public static String toString( String[] strings, String delimiter )
    {
        StringBuilder concat = new StringBuilder();
        boolean addDelimiter = false;

        for ( String string : strings )
        {
            if ( addDelimiter )
            {
                concat.append( delimiter );
            }
            else
            {
                addDelimiter = true;
            }

            concat.append( string );
        }
        return concat.toString();
    }

    /**
     * Filters a list based on the passed in function
     * @param <U> the type of object
     * @param source The list to filter
     * @param expression The expression used to determine what should be in the list
     * @return A new collection containing  all elements that passed through the filter
     */
    public static <U> Collection<U> where( Collection<U> source, Predicate<U> expression )
    {
        List<U> filteredValues = new ArrayList<>();

        if ( source != null )
        {
            for ( U sourceValue : source )
            {
                if ( expression.test( sourceValue ) )
                {
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
    public static <U> U find( Collection<U> source, Predicate<U> expression )
    {
        U val = null;
        for ( U value : source )
        {
            if ( expression.test( value ) )
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
    public static <U> boolean exists( Collection<U> source, Predicate<U> expression )
    {
        boolean exists = false;

        for ( U value : source )
        {
            if ( expression.test( value ) )
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

        for ( U arrayValue : array )
        {
            if ( ( Objects.isNull( value ) && Objects.isNull( arrayValue ) ) ||
                 arrayValue.equals( value ) )
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
    public static <U, V> U getKeyByValue( Map<U, V> mapping, V value )
    {
        U key = null;
        for ( Entry<U, V> entry : mapping.entrySet() )
        {
            if ( entry.getValue().equals( value ) )
            {
                key = entry.getKey();
                break;
            }
        }

        return key;
    }

    /**
     * Hidden constructor.
     */

    private Collections()
    {
    }

}
