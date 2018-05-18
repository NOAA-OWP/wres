package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 * 
 * @author james.brown@hydrosolved.com
 */
public class CoefficientOfDetermination extends CorrelationPearsons
{

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
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata( in.getSampleSize(),
                                                                                             in.getDimension(),
                                                                                             in.getInputDimension(),
                                                                                             MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                                             MetricConstants.MAIN,
                                                                                             in.getIdentifier() );
        return getDataFactory().ofDoubleScoreOutput(Math.pow(output.getData(), 2), meta);
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class CoefficientOfDeterminationBuilder extends CorrelationPearsonsBuilder
    {

        @Override
        public CoefficientOfDetermination build() throws MetricParameterException
        {
            return new CoefficientOfDetermination(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private CoefficientOfDetermination(final CoefficientOfDeterminationBuilder builder) throws MetricParameterException
    {
        super(builder);
    }    
    
}
