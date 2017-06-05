package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * The Probability of False Detection (PoD) measures the fraction of observed non-occurrences that were false alarms.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfFalseDetection<S extends DichotomousPairs, T extends ScalarOutput>
extends
    ContingencyTable<S, T>
implements Score, Collectable<S, MetricOutput<?>, T>
{

    /**
     * The metric name.
     */

    private static final String METRIC_NAME = "Probability of False Detection";

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ProbabilityOfFalseDetectionBuilder<S extends DichotomousPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
    {

        @Override
        public ProbabilityOfFalseDetection<S, T> build()
        {
            return new ProbabilityOfFalseDetection<>();
        }

    }

    @Override
    public T apply(final S s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public T apply(final MetricOutput<?> output)
    {
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = (MatrixOutput)output;
        final double[][] cm = v.getData().getDoubles();
        return MetricOutputFactory.ofExtendsScalarOutput(cm[0][1] / (cm[0][1] + cm[1][1]),
                                                         v.getSampleSize(),
                                                         output.getDimension());
    }

    @Override
    public String getName()
    {
        return METRIC_NAME;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public int getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public MetricOutput<?> getCollectionInput(final S input)
    {
        return super.apply(input); //2x2 contingency table
    }

    @Override
    public String getCollectionOf()
    {
        return super.getName();
    }

    /**
     * Hidden constructor.
     */

    private ProbabilityOfFalseDetection()
    {
        super();
    }

}
