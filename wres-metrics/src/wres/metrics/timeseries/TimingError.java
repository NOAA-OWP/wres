package wres.metrics.timeseries;

import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.Metric;

/**
 * Abstract base class for timing error metrics.
 * 
 * @author James Brown
 */
public abstract class TimingError implements Metric<Pool<TimeSeries<Pair<Double,Double>>>, DurationDiagramStatisticOuter>
{
    /**
     * A random number generator for resolving ties.
     */
    
    private final Random rng;

    @Override
    public String toString()
    {
        return this.getMetricName().toString();
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

    Random getRandomNumberGenerator()
    {
        return this.rng;
    }

}
