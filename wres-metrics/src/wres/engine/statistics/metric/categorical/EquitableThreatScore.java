package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 * 
 * @author james.brown@hydrosolved.com
 */
public class EquitableThreatScore extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public DoubleScoreOutput apply( final DichotomousPairs s )
    {
        return aggregate( this.getCollectionInput( s ) );
    }

    @Override
    public DoubleScoreOutput aggregate( final MatrixOutput output )
    {
        this.is2x2ContingencyTable( output, this );
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        final double t = cm[0][0] + cm[0][1] + cm[1][0];
        final double hitsRandom = ( ( cm[0][0] + cm[1][0] ) * ( cm[0][0] + cm[0][1] ) ) / ( t + cm[1][1] );
        double result =
                FunctionFactory.finiteOrNaN().applyAsDouble( ( cm[0][0] - hitsRandom ) / ( t - hitsRandom ) );
        return getDataFactory().ofDoubleScoreOutput( result, getMetadata( output ) );
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

    public static class EquitableThreatScoreBuilder extends OrdinaryScoreBuilder<DichotomousPairs, DoubleScoreOutput>
    {

        @Override
        public EquitableThreatScore build() throws MetricParameterException
        {
            return new EquitableThreatScore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private EquitableThreatScore( final EquitableThreatScoreBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
