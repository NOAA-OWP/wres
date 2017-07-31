package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * Computes the square of Pearson's product-moment correlation coefficient between the left and right sides of the
 * {SingleValuedPairs} input.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CoefficientOfDetermination extends CorrelationPearsons
{

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        return apply(getCollectionInput(s));
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.COEFFICIENT_OF_DETERMINATION;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricConstants getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class CoefficientOfDeterminationBuilder extends CorrelationPearsonsBuilder
    {

        @Override
        protected CorrelationPearsons build()
        {
            return new CoefficientOfDetermination(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private CoefficientOfDetermination(final CoefficientOfDeterminationBuilder b)
    {
        super(b);
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
        return super.apply(input);
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.CORRELATION_PEARSONS;
    }

}
