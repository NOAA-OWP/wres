package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Dimension;
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
public class SumOfSquareError extends DecomposableScore<SingleValuedPairs>
        implements Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static SumOfSquareError of()
    {
        return new SumOfSquareError();
    }
    
    @Override
    public DoubleScoreOutput apply( SingleValuedPairs s )
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
    public DoubleScoreOutput getInputForAggregation( SingleValuedPairs input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.MISSING_DOUBLE;

        // Data available
        if ( !input.getRawData().isEmpty() )
        {
            returnMe = input.getRawData().stream().mapToDouble( FunctionFactory.squareError() ).sum();
        }

        //Metadata
        final MetricOutputMetadata metOut = this.getMetadata( input );

        return DoubleScoreOutput.of( returnMe, metOut );
    }

    @Override
    public DoubleScoreOutput aggregate( DoubleScoreOutput output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        MetricOutputMetadata meta = MetricOutputMetadata.of( output.getMetadata().getSampleSize(),
        output.getMetadata().getDimension(),
        output.getMetadata().getInputDimension(),
        this.getID(),
        output.getMetadata().getMetricComponentID(),
        output.getMetadata().getIdentifier(),
        output.getMetadata().getTimeWindow() );
        return DoubleScoreOutput.of( output.getData(), meta );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.SUM_OF_SQUARE_ERROR;
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

        DatasetIdentifier identifier = metIn.getIdentifier();
        // Add the baseline scenario identifier
        if ( input.hasBaseline() )
        {
            identifier = DatasetIdentifier.of( identifier,
                                                               input.getMetadataForBaseline()
                                                                    .getIdentifier()
                                                                    .getScenarioID() );
        }
        // Set the output dimension
        Dimension outputDimension = Dimension.of();
        if ( hasRealUnits() )
        {
            outputDimension = metIn.getDimension();
        }
        final Dimension outputDim = outputDimension;
        final DatasetIdentifier identifier1 = identifier;
        return MetricOutputMetadata.of( input.getRawData().size(),
        outputDim,
        metIn.getDimension(),
        this.getID(),
        MetricConstants.MAIN,
        identifier1,
        metIn.getTimeWindow() );
    }

    /**
     * Hidden constructor.
     */

    SumOfSquareError()
    {
        super();
    }
    
    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
