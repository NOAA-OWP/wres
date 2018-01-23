package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class RootMeanSquareError extends DoubleErrorScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
{

    /**
     * Instance if {@link MeanSquareError}.
     */

    private final MeanSquareError<SingleValuedPairs> mse;

    @Override
    public DoubleScoreOutput apply( final SingleValuedPairs t )
    {
        return aggregate( getCollectionInput( t ) );
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
        final MetadataFactory f = getDataFactory().getMetadataFactory();
        // Set the output dimension
        Dimension outputDimension = f.getDimension();
        if( hasRealUnits() )
        {
            outputDimension = metIn.getDimension();
        }
        MetricOutputMetadata meta = f.getOutputMetadata( metIn.getSampleSize(),
                                                         outputDimension,
                                                         metIn.getDimension(),
                                                         getID(),
                                                         MetricConstants.MAIN,
                                                         metIn.getIdentifier(),
                                                         metIn.getTimeWindow() );
        return getDataFactory().ofDoubleScoreOutput( Math.sqrt( output.getData() ), meta );
    }

    @Override
    public DoubleScoreOutput getCollectionInput( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return mse.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class RootMeanSquareErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        public RootMeanSquareError build() throws MetricParameterException
        {
            return new RootMeanSquareError( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private RootMeanSquareError( final RootMeanSquareErrorBuilder builder ) throws MetricParameterException
    {
        super( builder.setErrorFunction( FunctionFactory.squareError() ) );
        mse = MetricFactory.getInstance( getDataFactory() ).ofMeanSquareError();
    }

}
