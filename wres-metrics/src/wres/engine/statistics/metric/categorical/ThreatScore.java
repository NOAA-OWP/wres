package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.FunctionFactory;

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
public class ThreatScore extends ContingencyTableScore
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ThreatScore of()
    {
        return new ThreatScore();
    }

    @Override
    public DoubleScoreStatistic apply( final SampleData<Pair<Boolean, Boolean>> s )
    {
        return aggregate( getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatistic aggregate( final DoubleScoreStatistic output )
    {
        is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getData();

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getData();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData();

        double result = FunctionFactory.finiteOrMissing().applyAsDouble( tP / ( tP + fP + fN ) );
        return DoubleScoreStatistic.of( result, getMetadata( output ) );
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
     * Hidden constructor.
     */

    private ThreatScore()
    {
        super();
    }

}
