package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */
public class MeanSquareError<S extends SingleValuedPairs> extends SumOfSquareError<S>
        implements Collectable<S, DoubleScoreOutput, DoubleScoreOutput>
{

    @Override
    public DoubleScoreOutput apply( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        switch ( getScoreOutputGroup() )
        {
            case NONE:
                return getMSENoDecomp( s );
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new MetricCalculationException( "The Mean Square Error decomposition is not currently "
                                                      + "implemented." );
        }
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        return output;
    }

    @Override
    public DoubleScoreOutput getCollectionInput( S input )
    {
        return apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorBuilder<S extends SingleValuedPairs>
            extends
            DecomposableScoreBuilder<S>
    {

        @Override
        public MeanSquareError<S> build() throws MetricParameterException
        {
            return new MeanSquareError<>( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    protected MeanSquareError( final MeanSquareErrorBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
    }

    /**
     * Returns the {@link MetricOutputMetadata} associated with the score.
     * 
     * @param input the input
     * @return the metdata
     */

    protected MetricOutputMetadata getMetadata( SingleValuedPairs input )
    {
        final Metadata metIn = input.getMetadata();
        final MetadataFactory f = getDataFactory().getMetadataFactory();
        DatasetIdentifier identifier = metIn.getIdentifier();
        // Add the baseline scenario identifier
        if ( input.hasBaseline() )
        {
            identifier = f.getDatasetIdentifier( identifier,
                                                 input.getMetadataForBaseline().getIdentifier().getScenarioID() );
        }
        // Set the output dimension
        Dimension outputDimension = f.getDimension();
        if ( hasRealUnits() )
        {
            outputDimension = metIn.getDimension();
        }
        return f.getOutputMetadata( input.getData().size(),
                                    outputDimension,
                                    metIn.getDimension(),
                                    getID(),
                                    MetricConstants.MAIN,
                                    identifier,
                                    metIn.getTimeWindow() );
    }

    /**
     * Returns the Mean Square Error without any decomposition.
     * 
     * @param s the pairs
     * @return the mean square error without decomposition
     */

    private DoubleScoreOutput getMSENoDecomp( final SingleValuedPairs s )
    {
        double mse = FunctionFactory.finiteOrNaN().applyAsDouble( getSumOfSquareError( s ) / s.getData().size() );
        return getDataFactory().ofDoubleScoreOutput( mse, getMetadata( s ) );
    }

}
