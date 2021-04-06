package wres.engine.statistics.metric;


import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.ScoreStatistic;

/**
 * An abstract score.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class OrdinaryScore<S extends Pool<?>, T extends ScoreStatistic<?,?>> implements Score<S, T>
{

    @Override
    public String toString()
    {
        return getMetricName().toString();
    }

}
