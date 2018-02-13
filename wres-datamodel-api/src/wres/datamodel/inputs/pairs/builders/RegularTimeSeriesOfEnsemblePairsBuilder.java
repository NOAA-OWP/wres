package wres.datamodel.inputs.pairs.builders;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * <p>A builder for a regular {@link TimeSeries} of {@link EnsemblePairs}.</p>
 * 
 * <p>Every time-series in the container must contain the same number of ensemble members, thereby allowing for 
 * iteration by ensemble trace.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public interface RegularTimeSeriesOfEnsemblePairsBuilder extends PairedInputBuilder<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Adds an atomic time-series to the builder. The values must be time-ordered, moving away from the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfEnsemblePairsBuilder addTimeSeriesData( Instant basisTime,
                                                                       List<PairOfDoubleAndVectorOfDoubles> values )
    {
        List<Event<List<PairOfDoubleAndVectorOfDoubles>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesData( input );
    }

    /**
     * Adds an atomic time-series to the builder for a baseline. The values must be time-ordered, moving away from 
     * the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfEnsemblePairsBuilder addTimeSeriesDataForBaseline( Instant basisTime,
                                                                                  List<PairOfDoubleAndVectorOfDoubles> values )
    {
        List<Event<List<PairOfDoubleAndVectorOfDoubles>>> input = new ArrayList<>();
        input.add( Event.of( basisTime, values ) );
        return addTimeSeriesDataForBaseline( input );
    }

    /**
     * Adds a list of atomic time-series to the builder, each one stored against its basis time. The values must be 
     * time-ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfEnsemblePairsBuilder
            addTimeSeriesData( List<Event<List<PairOfDoubleAndVectorOfDoubles>>> timeSeries );

    /**
     * Adds a list of atomic time-series to the builder for a baseline, each one stored against its basis time. The 
     * values must be time-ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfEnsemblePairsBuilder
            addTimeSeriesDataForBaseline( List<Event<List<PairOfDoubleAndVectorOfDoubles>>> timeSeries );

    /**
     * Adds a time-series to the builder.
     * 
     * @param timeSeries the regular time-series
     * @return the builder
     * @throws MetricInputException if the specified input is inconsistent with any existing input
     */

    RegularTimeSeriesOfEnsemblePairsBuilder addTimeSeries( TimeSeriesOfEnsemblePairs timeSeries );

    /**
     * Sets the time-step of the regular time-series.
     * 
     * @param timeStep the time-step of the regular time-series
     * @return the builder
     */

    RegularTimeSeriesOfEnsemblePairsBuilder setTimeStep( Duration timeStep );

    /**
     * Builds a time-series.
     * 
     * @return a time-series
     */

    TimeSeriesOfEnsemblePairs build();

}

