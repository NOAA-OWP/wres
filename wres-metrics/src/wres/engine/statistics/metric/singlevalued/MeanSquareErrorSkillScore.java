package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareErrorSkillScore<S extends SingleValuedPairs> extends DecomposableScore<S>
{

    /**
     * Instance if {@link SumOfSquareError}.
     */

    private final SumOfSquareError<SingleValuedPairs> sse;

    @Override
    public DoubleScoreOutput apply( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata

        double result = Double.NaN;

        // Some data, proceed
        if ( !s.getData().isEmpty() )
        {
            double numerator = sse.apply( s ).getData();
            double denominator = 0.0;
            if ( s.hasBaseline() )
            {
                denominator = sse.apply( s.getBaselineData() ).getData();
            }
            else
            {
                DataFactory d = getDataFactory();
                double meanLeft = FunctionFactory.mean().applyAsDouble( d.vectorOf( d.getSlicer().getLeftSide( s ) ) );
                for ( PairOfDoubles next : s.getData() )
                {
                    denominator += Math.pow( next.getItemOne() - meanLeft, 2 );
                }
            }
            result = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        // Metadata
        DatasetIdentifier baselineIdentifier = null;
        if ( s.hasBaseline() )
        {
            baselineIdentifier = s.getMetadataForBaseline().getIdentifier();
        }
        final MetricOutputMetadata metOut =
                this.getMetadata( s, s.getData().size(), MetricConstants.MAIN, baselineIdentifier );
        return this.getDataFactory().ofDoubleScoreOutput( result, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorSkillScoreBuilder<S extends SingleValuedPairs>
            extends
            DecomposableScoreBuilder<S>
    {

        @Override
        public MeanSquareErrorSkillScore<S> build() throws MetricParameterException
        {
            return new MeanSquareErrorSkillScore<>( this );
        }

    }

    /**
     * Prevent direct construction.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    protected MeanSquareErrorSkillScore( final MeanSquareErrorSkillScoreBuilder<S> builder )
            throws MetricParameterException
    {
        super( builder );
        sse = MetricFactory.getInstance( this.getDataFactory() ).ofSumOfSquareError();
    }

}
