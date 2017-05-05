package gov.noaa.wres.datamodel;

/**
 * Tuple for cases where non-primitive types are used.
 * 
 * For example, when having two sets of forecasts as a pair,
 * could create a Tuple<DoubleBrick,DoubleBrick>
 * 
 * @author jesse
 *
 * @param <T> type of first element
 * @param <U> type of second/last element
 */
public interface Tuple<T,U>
{
    T getFirst();
    U getSecond();
}
