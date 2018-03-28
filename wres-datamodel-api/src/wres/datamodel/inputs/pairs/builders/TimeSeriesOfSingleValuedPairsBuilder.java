package wres.datamodel.inputs.pairs.builders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A builder for a possibly irregular {@link TimeSeries} of {@link SingleValuedPairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

public interface TimeSeriesOfSingleValuedPairsBuilder extends PairedInputBuilder<PairOfDoubles>
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
        List<Event<List<Event<PairOfDoubles>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesData( input );
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
