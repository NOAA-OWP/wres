package wres.engine.statistics.metric;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Slicer;

/**
 * Computes Pearson's product-moment correlation coefficient between the left and right sides of the {SingleValuedPairs}
 * input. Implements {@link Collectable} to avoid repeated calculations of derivative metrics, such as the
 * {@link CoefficientOfDetermination} when both appear in a {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class CorrelationPearsons extends Metric<SingleValuedPairs, ScalarOutput>
implements Score, Collectable<SingleValuedPairs, ScalarOutput, ScalarOutput>
{

    /**
     * Instance of {@link PearsonsCorrelation}.
     */

    private final PearsonsCorrelation correlation;

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        try
        {
            Slicer slicer = getDataFactory().getSlicer();
            final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
            double returnMe =
                            correlation.correlation(slicer.getLeftSide(s.getData()), slicer.getRightSide(s.getData()));
            return getDataFactory().ofScalarOutput(returnMe, metOut);
        }
        catch(Exception e)
        {
            throw new MetricCalculationException("Error computing Pearson's correlation coefficient: "
                + e.getMessage());
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
        return MetricConstants.CORRELATION_PEARSONS;
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

    @Override
    public ScalarOutput apply(ScalarOutput output)
    {
        return output;
    }

    @Override
    public ScalarOutput getCollectionInput(SingleValuedPairs input)
    {
        return apply(input);
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return getID();
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class CorrelationPearsonsBuilder extends MetricBuilder<SingleValuedPairs, ScalarOutput>
    {

        @Override
        protected CorrelationPearsons build()
        {
            return new CorrelationPearsons(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    protected CorrelationPearsons(final CorrelationPearsonsBuilder b)
    {
        super(b.dataFactory);
        correlation = new PearsonsCorrelation();
    }

}
