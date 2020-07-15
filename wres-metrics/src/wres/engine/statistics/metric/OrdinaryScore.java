package wres.engine.statistics.metric;


import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.ScoreStatistic;

/**
 * An abstract score.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class OrdinaryScore<S extends SampleData<?>, T extends ScoreStatistic<?,?>> implements Score<S, T>
{

    @Override
    public String toString()
    {
        return getMetricName().toString();
    }

}
