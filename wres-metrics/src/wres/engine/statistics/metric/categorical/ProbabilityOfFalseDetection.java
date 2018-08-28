package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Probability of False Detection (PoD) measures the fraction of observed non-occurrences that were false alarms.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfFalseDetection extends ContingencyTableScore<DichotomousPairs>
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
    public DoubleScoreStatistic apply( final DichotomousPairs s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatistic aggregate( final MatrixStatistic output )
    {
        this.is2x2ContingencyTable( output, this );
        final MatrixStatistic v = output;
        final double[][] cm = v.getData().getDoubles();
        double result = FunctionFactory.finiteOrMissing().applyAsDouble( cm[0][1] / ( cm[0][1] + cm[1][1] ) );
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
