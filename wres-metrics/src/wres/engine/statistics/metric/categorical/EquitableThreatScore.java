package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 * 
 * @author james.brown@hydrosolved.com
 */
public class EquitableThreatScore extends ContingencyTableScore
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
    public DoubleScoreStatistic apply( final SampleData<Pair<Boolean,Boolean>> s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatistic aggregate( final DoubleScoreStatistic output )
    {
        this.is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getData();

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getData();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData();

        double tN = output.getComponent( MetricConstants.TRUE_NEGATIVES )
                          .getData();

        final double t = tP + fP + fN;
        final double hitsRandom = ( ( tP + fN ) * ( tP + fP ) ) / ( t + tN );
        double result =
                FunctionFactory.finiteOrMissing().applyAsDouble( ( tP - hitsRandom ) / ( t - hitsRandom ) );
        return DoubleScoreStatistic.of( result, getMetadata( output ) );
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
