package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 * 
 * @author james.brown@hydrosolved.com
 */
public class CoefficientOfDetermination extends CorrelationPearsons
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static CoefficientOfDetermination of()
    {
        return new CoefficientOfDetermination();
    }
    
    @Override
    public DoubleScoreOutput apply(SingleValuedPairs s)
    {
        return aggregate(getInputForAggregation(s));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public DoubleScoreOutput aggregate(DoubleScoreOutput output)
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        MetricOutputMetadata in = output.getMetadata();
        MetricOutputMetadata meta = MetadataFactory.getOutputMetadata( in.getSampleSize(),
                                                                                             in.getDimension(),
                                                                                             in.getInputDimension(),
                                                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                                             MetricConstants.MAIN,
                                                                                             in.getIdentifier() );
        return DoubleScoreOutput.of( Math.pow(output.getData(), 2), meta );
    }

    @Override
    public DoubleScoreOutput getInputForAggregation(SingleValuedPairs input)
    {
        if(Objects.isNull(input))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        return super.apply(input);
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * Hidden constructor.
     */

    private CoefficientOfDetermination()
    {
        super();
    }    
    
}
