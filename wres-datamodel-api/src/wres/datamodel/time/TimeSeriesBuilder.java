package wres.datamodel.time;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.inputs.MetricInputException;

/**
 * <p>A builder for a possibly irregular {@link TimeSeries}.
 * 
 * @param <T> the atomic type of data
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesBuilder<T>
{

    /**
     * Adds an atomic time-series to the builder.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the pairs of time-series values, ordered from earliest to latest
     * @return the builder
     */

    default TimeSeriesBuilder<T> addTimeSeriesData( Instant basisTime, List<Event<T>> values )
    {
        List<Event<List<Event<T>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesData( input );
    }

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     * @throws NullPointerException if the input is null
     */

    default TimeSeriesBuilder<T> addTimeSeries( TimeSeries<T> timeSeries )
    {
        Objects.requireNonNull( timeSeries, "Specify non-null time-series input." );

        for ( TimeSeries<T> next : timeSeries.basisTimeIterator() )
        {
            Instant basisTime = next.getEarliestBasisTime();
            List<Event<T>> values = new ArrayList<>();
            next.timeIterator().forEach( values::add );
            this.addTimeSeriesData( basisTime, values );
        }

        return this;
    }

    /**
     * Adds a list of atomic time-series to the builder, each one stored against its basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    TimeSeriesBuilder<T>
            addTimeSeriesData( List<Event<List<Event<T>>>> timeSeries );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeries<T> build();

}
