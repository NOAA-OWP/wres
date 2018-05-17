package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanSquareError<S extends SingleValuedPairs> extends SumOfSquareError<S>
{

    @Override
    public DoubleScoreOutput apply( final S s )
    {
        switch ( this.getScoreOutputGroup() )
        {
            case NONE:
                return this.aggregate( this.getInputForAggregation( s ) );
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new MetricCalculationException( "Decomposition is not currently implemented for the '" + this
                                                      + "'." );
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
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        final MetricOutputMetadata metIn = output.getMetadata();
        final MetadataFactory f = getDataFactory().getMetadataFactory();

        // Set the output dimension
        Dimension outputDimension = f.getDimension();
        if ( hasRealUnits() )
        {
            outputDimension = metIn.getDimension();
        }
        MetricOutputMetadata meta = f.getOutputMetadata( metIn.getSampleSize(),
                                                         outputDimension,
                                                         metIn.getDimension(),
                                                         this.getID(),
                                                         MetricConstants.MAIN,
                                                         metIn.getIdentifier(),
                                                         metIn.getTimeWindow() );

        double mse = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( output.getData() / metIn.getSampleSize() );

        return this.getDataFactory().ofDoubleScoreOutput( mse, meta );
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorBuilder<S extends SingleValuedPairs>
            extends
            SumOfSquareErrorBuilder<S>
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

}
