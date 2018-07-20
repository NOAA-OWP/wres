package wres.engine.statistics.metric;


import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.ScoreOutput;

/**
 * An abstract score.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class OrdinaryScore<S extends MetricInput<?>, T extends ScoreOutput<?,T>> implements Score<S, T>
{

    @Override
    public String toString()
    {
        return getID().toString();
    }

}
