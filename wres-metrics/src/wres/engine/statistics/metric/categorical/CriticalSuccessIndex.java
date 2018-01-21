package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>
 * The Critical Success Index (CSI) or "threat score" is a dichotomous measure of the fraction of all predicted outcomes
 * that occurred (i.e. were true positives).
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class CriticalSuccessIndex extends ContingencyTableScore<DichotomousPairs>
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
        return getDataFactory().ofScalarOutput(cm[0][0] / (cm[0][0] + cm[0][1] + cm[1][0]), getMetadata(output));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.CRITICAL_SUCCESS_INDEX;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class CriticalSuccessIndexBuilder extends OrdinaryScoreBuilder<DichotomousPairs, ScalarOutput>
    {

        @Override
        public CriticalSuccessIndex build() throws MetricParameterException
        {
            return new CriticalSuccessIndex(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private CriticalSuccessIndex(final CriticalSuccessIndexBuilder builder) throws MetricParameterException
    {
        super(builder);
    }

}
