package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Random;

import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Abstract base class for timing error metrics.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class TimingError implements Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
{

    /**
     * A random number generator for resolving ties.
     */
    
    private final Random rng;

    @Override
    public String toString()
    {
        return getID().toString();
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public abstract static class TimingErrorBuilder
            implements MetricBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
    {

        /**
         * A random number generator for resolving ties.
         */
        
        private Random rng;
        
        /**
         * Optionally, assign a random number generator for resolving ties.
         * 
         * @param rng the random number generator
         * @return the builder
         */
        TimingErrorBuilder setRNGForTies( Random rng )
        {
            this.rng = rng;
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    TimingError( final TimingErrorBuilder builder ) throws MetricParameterException
    {
        if ( Objects.isNull( builder ) )
        {
            throw new MetricParameterException( "Cannot construct the metric with a null builder." );
        }
        
        if ( Objects.nonNull( builder.rng ) )
        {
            rng = builder.rng;
        }
        else
        {
            rng = new Random();
        }
    }
    
    /**
     * Returns the random number generator used to resolve ties.
     * 
     * @return the random number generator
     */

    Random getRNG()
    {
        return rng;
    }

}
