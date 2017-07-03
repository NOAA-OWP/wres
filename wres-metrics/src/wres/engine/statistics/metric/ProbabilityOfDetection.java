package wres.engine.statistics.metric;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.ScalarOutput;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfDetection extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public ScalarOutput apply(final DichotomousPairs s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public ScalarOutput apply(final MatrixOutput output)
    {
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        return getOutputFactory().ofScalarOutput(cm[0][0] / (cm[0][0] + cm[1][0]), getMetadata(output));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PROBABILITY_OF_DETECTION;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class ProbabilityOfDetectionBuilder extends MetricBuilder<DichotomousPairs, ScalarOutput>
    {

        @Override
        protected ProbabilityOfDetection build()
        {
            return new ProbabilityOfDetection(outputFactory);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param outputFactory the {@link MetricOutputFactory}.
     */

    private ProbabilityOfDetection(final MetricOutputFactory outputFactory)
    {
        super(outputFactory);
    }
}
