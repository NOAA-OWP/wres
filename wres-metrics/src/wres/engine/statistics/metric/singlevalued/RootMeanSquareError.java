package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Dimension;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 * 
 * @author james.brown@hydrosolved.com
 */
public class RootMeanSquareError extends DoubleErrorScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static RootMeanSquareError of()
    {
        return new RootMeanSquareError();
    }
    
    /**
     * Instance if {@link SumOfSquareError}.
     */

    private final SumOfSquareError sse;

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs t )
    {
        return this.aggregate( this.getInputForAggregation( t ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        final MetricOutputMetadata metIn = output.getMetadata();

        // Set the output dimension
        Dimension outputDimension = metIn.getDimension();

        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( metIn.getSampleSize(),
                                                                       outputDimension,
                                                                       metIn.getDimension(),
                                                                       this.getID(),
                                                                       MetricConstants.MAIN,
                                                                       metIn.getIdentifier(),
                                                                       metIn.getTimeWindow() );
        return DoubleScoreOutput.of( Math.sqrt( output.getData() / metIn.getSampleSize() ), meta );
    }

    @Override
    public DoubleScoreOutput getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return sse.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private RootMeanSquareError()
    {
        super( FunctionFactory.squareError() );
        
        sse = SumOfSquareError.of();
    }

}
