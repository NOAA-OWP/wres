package wres.engine.statistics.metric;

import wres.datamodel.DichotomousPairs;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetricConstants;
import wres.datamodel.ScalarOutput;

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
class CriticalSuccessIndex extends ContingencyTableScore<DichotomousPairs>

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

    static class CriticalSuccessIndexBuilder extends MetricBuilder<DichotomousPairs, ScalarOutput>
    {

        @Override
        protected CriticalSuccessIndex build()
        {
            return new CriticalSuccessIndex(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the {@link builder}.
     */

    private CriticalSuccessIndex(final CriticalSuccessIndexBuilder builder)
    {
        super(builder);
    }

}
