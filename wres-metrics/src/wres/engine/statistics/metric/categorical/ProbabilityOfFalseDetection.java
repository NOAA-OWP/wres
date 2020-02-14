package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Probability of False Detection (PoD) measures the fraction of observed non-occurrences that were false alarms.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfFalseDetection extends ContingencyTableScore
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static ProbabilityOfFalseDetection of()
    {
        return new ProbabilityOfFalseDetection();
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

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getData();

        double tN = output.getComponent( MetricConstants.TRUE_NEGATIVES )
                          .getData();

        double result = FunctionFactory.finiteOrMissing().applyAsDouble( fP / ( fP + tN ) );
        return DoubleScoreStatistic.of( result, getMetadata( output ) );
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
     * Hidden constructor.
     */

    private ProbabilityOfFalseDetection()
    {
        super();
    }
}
