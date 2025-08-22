package wres.vis.data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

/**
 * Overrides a non-performant method in a {@link TimeSeriesCollection}.
 *
 * @author James Brown
 */
class PerformantTimeSeriesCollection extends TimeSeriesCollection // NOSONAR
{
    /** The internal store of series, mapped by name. */
    private final Map<String, TimeSeries> timeSeries;

    /**
     * Creates an instance.
     * @param timeSeries the time-series
     * @return the collection
     * @throws NullPointerException if the input is null
     */

    static PerformantTimeSeriesCollection of( List<TimeSeries> timeSeries )
    {
        return new PerformantTimeSeriesCollection( timeSeries );
    }

    @Override
    public TimeSeries getSeries( Comparable key )
    {
        Objects.requireNonNull( key );
        return this.timeSeries.get( key.toString() );
    }

    /**
     * Hidden constructor.
     * @param timeSeries the time-series
     */
    private PerformantTimeSeriesCollection( List<TimeSeries> timeSeries )
    {
        timeSeries.forEach( this::addSeries );
        this.timeSeries = new HashMap<>();
        timeSeries.forEach( s -> this.timeSeries.put( s.getKey()
                                                       .toString(), s ) );
    }

    @Override
    public boolean equals( Object another )
    {
        return super.equals( another );
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

}
