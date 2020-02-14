package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfDetection extends ContingencyTableScore
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static ProbabilityOfDetection of()
    {
        return new ProbabilityOfDetection();
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

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData();
        
        double result = FunctionFactory.finiteOrMissing().applyAsDouble( tP / ( tP + fN ) );
        return DoubleScoreStatistic.of( result, getMetadata( output ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PROBABILITY_OF_DETECTION;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private ProbabilityOfDetection()
    {
        super();
    }
}
