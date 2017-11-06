package wres.datamodel.inputs.pairs;

/**
 * Pair for cases where non-primitive types are used.
 *
 * For example, when having two sets of forecasts as a pair,
 * could create a {@code Pair<DoubleBrick,DoubleBrick>}
 *
 * @author jesse
 *
 * @param <T> type of first element
 * @param <U> type of second/last element
 */
public interface Pair<T,U>
{
    /**
     * Get the first value
     * @return the first instance in this tuple
     */
    T getItemOne();
    
    /**
     * Get the second value
     * @return the second instance in this tuple
     */
    U getItemTwo();
}
