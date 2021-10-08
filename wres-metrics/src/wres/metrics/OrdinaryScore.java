package wres.metrics;


import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.ScoreStatistic;

/**
 * An abstract score.
 * 
 * @author James Brown
 */

public abstract class OrdinaryScore<S extends Pool<?>, T extends ScoreStatistic<?,?>> implements Score<S, T>
{

    @Override
    public String toString()
    {
        return getMetricName().toString();
    }

}
