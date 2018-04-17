package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SumOfSquareError<S extends SingleValuedPairs> extends DecomposableScore<S>
        implements Collectable<S, DoubleScoreOutput, DoubleScoreOutput>
{

    @Override
    public DoubleScoreOutput apply( S s )
    {
        return this.aggregate( this.getInputForAggregation( s ) );
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
    public MetricConstants getID()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    @Override
    public DoubleScoreOutput getInputForAggregation( S input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( input.getData().size() > 0 )
        {
            returnMe = input.getData().stream().mapToDouble( FunctionFactory.squareError() ).sum();
        }

        //Metadata
        final MetricOutputMetadata metOut = this.getMetadata( input );

        return this.getDataFactory().ofDoubleScoreOutput( returnMe, metOut );
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        final MetadataFactory f = getDataFactory().getMetadataFactory();
        MetricOutputMetadata meta = f.getOutputMetadata( output.getMetadata().getSampleSize(),
                                                         output.getMetadata().getDimension(),
                                                         output.getMetadata().getInputDimension(),
                                                         this.getID(),
                                                         output.getMetadata().getMetricComponentID(),
                                                         output.getMetadata().getIdentifier(),
                                                         output.getMetadata().getTimeWindow() );
        return this.getDataFactory().ofDoubleScoreOutput( output.getData(), meta );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class SumOfSquareErrorBuilder<S extends SingleValuedPairs> extends DecomposableScoreBuilder<S>
    {

        @Override
        public SumOfSquareError<S> build() throws MetricParameterException
        {
            return new SumOfSquareError<>( this );
        }

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
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    identifier,
                                    metIn.getTimeWindow() );
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( final SumOfSquareErrorBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
    }

}
