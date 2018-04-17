package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfDetection extends ContingencyTableScore<DichotomousPairs>
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
        double result = FunctionFactory.finiteOrMissing().applyAsDouble( cm[0][0] / ( cm[0][0] + cm[1][0] ) );
        return getDataFactory().ofDoubleScoreOutput( result, getMetadata( output ) );
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ProbabilityOfDetectionBuilder extends OrdinaryScoreBuilder<DichotomousPairs, DoubleScoreOutput>
    {

        @Override
        public ProbabilityOfDetection build() throws MetricParameterException
        {
            return new ProbabilityOfDetection( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private ProbabilityOfDetection( final ProbabilityOfDetectionBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }
}
