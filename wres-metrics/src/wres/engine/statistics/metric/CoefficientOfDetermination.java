package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class CoefficientOfDetermination extends CorrelationPearsons
{

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public ScalarOutput apply(ScalarOutput output)
    {
        MetricOutputMetadata in = output.getMetadata();
        MetricOutputMetadata meta = getDataFactory().getMetadataFactory().getOutputMetadata(in.getSampleSize(),
                                                                                            in.getDimension(),
                                                                                            in.getInputDimension(),
                                                                                            getID(),
                                                                                            MetricConstants.MAIN,
                                                                                            in.getIdentifier());
        return getDataFactory().ofScalarOutput(Math.pow(output.getData(), 2), meta);
    }

    @Override
    public ScalarOutput getCollectionInput(SingleValuedPairs input)
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

    static class CoefficientOfDeterminationBuilder extends CorrelationPearsonsBuilder
    {

        @Override
        protected CoefficientOfDetermination build() throws MetricParameterException
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
