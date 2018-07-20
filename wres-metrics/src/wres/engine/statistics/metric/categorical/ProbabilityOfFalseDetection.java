package wres.engine.statistics.metric.categorical;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Probability of False Detection (PoD) measures the fraction of observed non-occurrences that were false alarms.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfFalseDetection extends ContingencyTableScore<DichotomousPairs>
{

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
        double result = FunctionFactory.finiteOrMissing().applyAsDouble( cm[0][1] / ( cm[0][1] + cm[1][1] ) );
        return DataFactory.ofDoubleScoreOutput( result, getMetadata( output ) );
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ProbabilityOfFalseDetectionBuilder implements MetricBuilder<DichotomousPairs, DoubleScoreOutput>
    {

        @Override
        public ProbabilityOfFalseDetection build() throws MetricParameterException
        {
            return new ProbabilityOfFalseDetection( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private ProbabilityOfFalseDetection( final ProbabilityOfFalseDetectionBuilder builder )
            throws MetricParameterException
    {
        super();
    }
}
