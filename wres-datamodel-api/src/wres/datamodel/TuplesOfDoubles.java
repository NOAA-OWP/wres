package wres.datamodel;

import java.util.List;

/**
 * Represents a List of TupleOfDoubles instances.
 *
 * An example might be the simplest forecast/observation timeseries data,
 * but stripped of any/all time information. Only the values.
 *
 * @author jesse
 *
 */
public interface TuplesOfDoubles
{
    List<TupleOfDoubles> getTuplesOfDoubles();
}
