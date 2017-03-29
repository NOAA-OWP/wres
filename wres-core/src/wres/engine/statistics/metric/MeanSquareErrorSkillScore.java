package wres.engine.statistics.metric;

import java.util.Objects;

import wres.engine.statistics.metric.inputs.MetricInputException;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareErrorSkillScore<S extends SingleValuedPairs, T extends MetricOutput>
extends
    MeanSquareError<S, T>
{

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @param <X> the single-valued pairs
     * @param <Y> the metric output
     * @return a default {@link MeanSquareErrorSkillScore} function.
     */

    public static <X extends SingleValuedPairs, Y extends MetricOutput> MeanSquareErrorSkillScore<X, Y> newInstance()
    {
        return new MeanSquareErrorSkillScore();
    }

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        if(!s.hasBaseline())
        {
            throw new MetricInputException("Specify a non-null baseline for the '" + toString() + "'.");
        }
        //TODO: implement any required decompositions, based on the instance parameters  
        final ScalarOutput numerator = (ScalarOutput)super.apply(s);
        final ScalarOutput denominator = (ScalarOutput)super.apply((S)s.getBaseline());
        return (T)new ScalarOutput(FunctionFactory.skill().applyAsDouble(numerator.valueOf(), denominator.valueOf()),
                                   s.size(),
                                   s.getDimension());
    }

    @Override
    public void checkParameters(final MetricParameter... par)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getName()
    {
        return "Mean Square Error Skill Score";
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    /**
     * Prevent direct construction.
     */

    protected MeanSquareErrorSkillScore()
    {
        super();
    }

}
