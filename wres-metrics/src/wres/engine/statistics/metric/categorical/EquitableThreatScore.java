package wres.engine.statistics.metric.categorical;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 * 
 * @author james.brown@hydrosolved.com
 */
public class EquitableThreatScore extends ContingencyTableScore<DichotomousPairs>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static EquitableThreatScore of()
    {
        return new EquitableThreatScore();
    }
    
    @Override
    public DoubleScoreOutput apply( final DichotomousPairs s )
    {
        return aggregate( this.getInputForAggregation( s ) );
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
                FunctionFactory.finiteOrMissing().applyAsDouble( ( cm[0][0] - hitsRandom ) / ( t - hitsRandom ) );
        return DataFactory.ofDoubleScoreOutput( result, getMetadata( output ) );
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
     * Hidden constructor.
     */

    private EquitableThreatScore()
    {
        super();
    }

}
