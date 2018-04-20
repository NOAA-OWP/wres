package wres.datamodel.inputs.pairs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.MetricInputBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A builder for a possibly irregular {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesOfEnsemblePairsBuilder extends MetricInputBuilder<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Adds an atomic time-series to the builder.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the pairs of time-series values, ordered from earliest to latest
     * @return the builder
     */

    default TimeSeriesOfEnsemblePairsBuilder addTimeSeriesData( Instant basisTime,
                                                                List<Event<PairOfDoubleAndVectorOfDoubles>> values )
    {
        List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> input = new ArrayList<>();
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

    default TimeSeriesOfEnsemblePairsBuilder addTimeSeriesDataForBaseline( Instant basisTime,
                                                                           List<Event<PairOfDoubleAndVectorOfDoubles>> values )
    {
        List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesDataForBaseline( input );
    }

    /**
     * Adds a list of atomic time-series to the builder, each one stored against its basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    TimeSeriesOfEnsemblePairsBuilder
            addTimeSeriesData( List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> timeSeries );

    /**
     * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    TimeSeriesOfEnsemblePairsBuilder
            addTimeSeriesDataForBaseline( List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> timeSeries );

    /**
     * Adds a time-series to the builder, including any baseline.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    TimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries );
    
    /**
     * Adds a time-series to the builder as a baseline dataset only. Any data associated with the 
     * {@link TimeSeriesOfEnsemblePairs#getBaselineData()} of the input is ignored.
     * 
     * @param timeSeries the time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    TimeSeriesOfEnsemblePairsBuilder addTimeSeriesForBaseline( TimeSeriesOfEnsemblePairs timeSeries );    

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeriesOfEnsemblePairs build();

}
