package wres.engine.statistics.metric;

import java.util.Objects;

import wres.engine.statistics.metric.inputs.MetricInputException;
import wres.engine.statistics.metric.inputs.MetricInputFactory;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareErrorSkillScore<S extends SingleValuedPairs, T extends VectorOutput>
extends
    MeanSquareError<S, T>
{

    /**
     * The metric name.
     */

    private static final String METRIC_NAME = "Mean Square Error Skill Score";

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        if(!s.hasTwo())
        {
            throw new MetricInputException("Specify a non-null baseline for the '" + toString() + "'.");
        }
        //TODO: implement any required decompositions, based on the instance parameters  
        final VectorOutput numerator = super.apply(s);
        final VectorOutput denominator = super.apply(MetricInputFactory.ofExtendsSingleValuedPairs(s.get(1),
                                                                                                   s.getDimension()));
        final double[] result = new double[]{
            FunctionFactory.skill().applyAsDouble(numerator.getData().getDoubles()[0],
                                                  denominator.getData().getDoubles()[0])};
        return MetricOutputFactory.ofExtendsVectorOutput(result, s.get(0).size(), s.getDimension());
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorSkillScoreBuilder<S extends SingleValuedPairs, T extends VectorOutput>
    extends
        MeanSquareErrorBuilder<S, T>
    {

        @Override
        public MeanSquareErrorSkillScore<S, T> build()
        {
            return new MeanSquareErrorSkillScore<>(this);
        }

    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    /**
     * Prevent direct construction.
     * 
     * @param b the builder
     */

    protected MeanSquareErrorSkillScore(final MeanSquareErrorSkillScoreBuilder<S, T> b)
    {
        super(b);
    }

}
