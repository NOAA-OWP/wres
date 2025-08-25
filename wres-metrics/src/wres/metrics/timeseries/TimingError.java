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
public abstract class TimingError
        implements Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter>
{
    /**
     * A seed for a random number generator, used to resolve ties.
     */

    private final Long seed;

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
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
        this.seed = null;
    }

    /**
     * Hidden constructor.
     *
     * @param seed the seed for the random number generator
     */

    TimingError( Long seed )
    {
        if ( Objects.nonNull( seed ) )
        {
            this.seed = seed;
        }
        else
        {
            this.seed = null;
        }
    }

    /**
     * Returns the random number generator, which is used to resolve ties.
     *
     * @return the random number generator
     */

    Random getRandomNumberGenerator()
    {
        if ( Objects.isNull( this.seed ) )
        {
            return new Random();
        }

        return new Random( this.seed );
    }

}
