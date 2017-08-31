package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.DichotomousPairs;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInputException;
import wres.datamodel.ScalarOutput;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class EquitableThreatScore extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public ScalarOutput apply(final DichotomousPairs s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public ScalarOutput apply(final MatrixOutput output)
    {
        if(Objects.isNull(output))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        is2x2ContingencyTable(output, this);
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        final double t = cm[0][0] + cm[0][1] + cm[1][0];
        final double hitsRandom = ((cm[0][0] + cm[1][0]) * (cm[0][0] + cm[0][1])) / (t + cm[1][1]);
        return getDataFactory().ofScalarOutput((cm[0][0] - hitsRandom) / (t - hitsRandom), getMetadata(output));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.EQUITABLE_THREAT_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class EquitableThreatScoreBuilder extends MetricBuilder<DichotomousPairs, ScalarOutput>
    {

        @Override
        protected EquitableThreatScore build()
        {
            return new EquitableThreatScore(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     */

    private EquitableThreatScore(final EquitableThreatScoreBuilder builder)
    {
        super(builder);
    }

}
