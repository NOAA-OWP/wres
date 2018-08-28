package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Random;

import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.PairedStatistic;
import wres.engine.statistics.metric.Metric;

/**
 * Abstract base class for timing error metrics.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class TimingError implements Metric<TimeSeriesOfSingleValuedPairs, PairedStatistic<Instant, Duration>>
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
     * Hidden constructor.
     */

    TimingError()
    {
        this.rng = new Random();
    }
    
    /**
     * Hidden constructor.
     * 
     * @param rng the random number generator for resolving ties 
     */

    TimingError( Random rng )
    {
        if ( Objects.nonNull( rng ) )
        {
            this.rng = rng;
        }
        else
        {
            this.rng = new Random();
        }
    }
    
    /**
     * Returns the random number generator used to resolve ties.
     * 
     * @return the random number generator
     */

    Random getRNG()
    {
        return this.rng;
    }

}
