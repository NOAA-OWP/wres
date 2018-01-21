package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Probability of False Detection (PoD) measures the fraction of observed non-occurrences that were false alarms.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ProbabilityOfFalseDetection extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public ScalarOutput apply(final DichotomousPairs s)
    {
        return aggregate(getCollectionInput(s));
    }

    @Override
    public ScalarOutput aggregate(final MatrixOutput output)
    {
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        return getDataFactory().ofScalarOutput(cm[0][1] / (cm[0][1] + cm[1][1]), getMetadata(output));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PROBABILITY_OF_FALSE_DETECTION;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ProbabilityOfFalseDetectionBuilder extends OrdinaryScoreBuilder<DichotomousPairs, ScalarOutput>
    {

        @Override
        public ProbabilityOfFalseDetection build() throws MetricParameterException
        {
            return new ProbabilityOfFalseDetection(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private ProbabilityOfFalseDetection(final ProbabilityOfFalseDetectionBuilder builder) throws MetricParameterException
    {
        super(builder);
    }
}
