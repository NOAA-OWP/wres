package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfDetection extends ContingencyTableScore<DichotomousPairs>
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
        double result = FunctionFactory.finiteOrMissing().applyAsDouble( cm[0][0] / ( cm[0][0] + cm[1][0] ) );
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
