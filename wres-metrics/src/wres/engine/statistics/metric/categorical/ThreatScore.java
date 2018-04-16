package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>
 * The Threat Score or Critical Success Index (CSI) measures the fraction of hits against observed occurrences 
 * (hits + misses) and observed non-occurrences that were predicted incorrectly (false alarms). It measures the 
 * accuracy of a set of predictions at detecting observed occurrences, removing the possibly large number of observed
 * non-occurrences that were predicted correctly.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class ThreatScore extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public DoubleScoreOutput apply( final DichotomousPairs s )
    {
        return aggregate( getCollectionInput( s ) );
    }

    @Override
    public DoubleScoreOutput aggregate( final MatrixOutput output )
    {
        is2x2ContingencyTable( output, this );
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        double result = FunctionFactory.finiteOrNaN().applyAsDouble( cm[0][0] / ( cm[0][0] + cm[0][1] + cm[1][0] ) );
        return getDataFactory().ofDoubleScoreOutput( result, getMetadata( output ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.THREAT_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ThreatScoreBuilder extends OrdinaryScoreBuilder<DichotomousPairs, DoubleScoreOutput>
    {

        @Override
        public ThreatScore build() throws MetricParameterException
        {
            return new ThreatScore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private ThreatScore( final ThreatScoreBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
