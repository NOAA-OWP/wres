package wres.datamodel.inputs.pairs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.inputs.MetricInputBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesBuilder;

/**
 * <p>A builder for a possibly irregular {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesOfSingleValuedPairsBuilder
        extends MetricInputBuilder<PairOfDoubles>, TimeSeriesBuilder<PairOfDoubles>
{

    /**
     * Adds an atomic time-series to the builder.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the pairs of time-series values, ordered from earliest to latest
     * @return the builder
     */

    default TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesData( Instant basisTime,
                                                                    List<Event<PairOfDoubles>> values )
    {
        TimeSeriesBuilder.super.addTimeSeriesData( basisTime, values );
        return this;
    }

    /**
     * Adds an atomic time-series to the builder for a baseline.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the pairs of time-series values, ordered from earliest to latest
     * @return the builder
     */

    default TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesDataForBaseline( Instant basisTime,
                                                                               List<Event<PairOfDoubles>> values )
    {
        List<Event<List<Event<PairOfDoubles>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesDataForBaseline( input );
    }

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     * @throws NullPointerException if the input is null
     */

    default TimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeries<PairOfDoubles> timeSeries )
    {
        TimeSeriesBuilder.super.addTimeSeries( timeSeries );
        return this;
    }

    /**
     * Adds a time-series to the builder for a baseline dataset.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     * @throws NullPointerException if the input is null
     */

    default TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeries<PairOfDoubles> timeSeries )
    {
        Objects.requireNonNull( timeSeries, "Specify non-null time-series input." );

        for ( TimeSeries<PairOfDoubles> next : timeSeries.basisTimeIterator() )
        {
            Instant basisTime = next.getEarliestBasisTime();
            List<Event<PairOfDoubles>> values = new ArrayList<>();
            next.timeIterator().forEach( values::add );
            this.addTimeSeriesDataForBaseline( basisTime, values );
        }

        return this;
    }

    /**
     * Adds a list of atomic time-series to the builder, each one stored against its basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    TimeSeriesOfSingleValuedPairsBuilder
            addTimeSeriesData( List<Event<List<Event<PairOfDoubles>>>> timeSeries );

    /**
     * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    TimeSeriesOfSingleValuedPairsBuilder
            addTimeSeriesDataForBaseline( List<Event<List<Event<PairOfDoubles>>>> timeSeries );

    /**
     * Adds a time-series to the builder, including any baseline.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    TimeSeriesOfSingleValuedPairsBuilder addTimeSeries( TimeSeriesOfSingleValuedPairs timeSeries );

    /**
     * Adds a time-series to the builder as a baseline dataset only. Any data associated with the 
     * {@link TimeSeriesOfSingleValuedPairs#getBaselineData()} of the input is ignored.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    TimeSeriesOfSingleValuedPairsBuilder addTimeSeriesForBaseline( TimeSeriesOfSingleValuedPairs timeSeries );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeriesOfSingleValuedPairs build();

}
