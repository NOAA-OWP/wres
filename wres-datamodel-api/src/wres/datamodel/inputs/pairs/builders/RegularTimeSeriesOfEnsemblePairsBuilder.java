package wres.datamodel.inputs.pairs.builders;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
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
     * Adds an atomic time-series to the builder.  The values must be time-ordered, moving away from the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfEnsemblePairsBuilder addData( Instant basisTime,
                                                             List<PairOfDoubleAndVectorOfDoubles> values )
    {
        Map<Instant, List<PairOfDoubleAndVectorOfDoubles>> input = new HashMap<>();
        input.put( basisTime, values );
        return addData( input );
    }

    /**
     * Adds an atomic time-series to the builder for a baseline.  The values must be time-ordered, moving away from 
     * the basis time.
     * 
     * @param basisTime the basis time for the time-series
     * @param values the time-series values, ordered from earliest to latest
     * @return the builder
     */

    default RegularTimeSeriesOfEnsemblePairsBuilder addDataForBaseline( Instant basisTime,
                                                                        List<PairOfDoubleAndVectorOfDoubles> values )
    {
        Map<Instant, List<PairOfDoubleAndVectorOfDoubles>> input = new HashMap<>();
        input.put( basisTime, values );
        return addDataForBaseline( input );
    }

    /**
     * Adds several time-series to the builder, each one stored against its basis time.  The values must be time-
     * ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfEnsemblePairsBuilder
            addData( Map<Instant, List<PairOfDoubleAndVectorOfDoubles>> timeSeries );

    /**
     * Adds several time-series to the builder for a baseline, each one stored against its basis time. The values must
     * be time-ordered, moving away from the basis time.
     * 
     * @param timeSeries the time-series, stored against their basis times
     * @return the builder
     */

    RegularTimeSeriesOfEnsemblePairsBuilder
            addDataForBaseline( Map<Instant, List<PairOfDoubleAndVectorOfDoubles>> timeSeries );

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

