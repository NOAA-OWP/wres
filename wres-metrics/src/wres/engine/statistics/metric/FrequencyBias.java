package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.ScalarOutput;

/**
 * Measures the predicted fraction of occurrences against the observed fraction of occurrences. A ratio of 1.0 
 * indicates an absence of any bias in the predicted and observed frequencies with which an event occurs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class FrequencyBias extends ContingencyTableScore<DichotomousPairs>
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
        final double score = (cm[0][0] + cm[0][1]) / (cm[0][0] + cm[1][0]);
        return getDataFactory().ofScalarOutput(score, getMetadata(output));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.FREQUENCY_BIAS;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class FrequencyBiasBuilder extends MetricBuilder<DichotomousPairs, ScalarOutput>
    {
        @Override
        protected FrequencyBias build() throws MetricParameterException
        {
            return new FrequencyBias(this);
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private FrequencyBias(final FrequencyBiasBuilder builder) throws MetricParameterException
    {
        super(builder);
    }

}
