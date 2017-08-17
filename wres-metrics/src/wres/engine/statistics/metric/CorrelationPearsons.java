package wres.engine.statistics.metric;

import java.util.Objects;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
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
class CorrelationPearsons extends Metric<SingleValuedPairs, ScalarOutput>
implements Score, Collectable<SingleValuedPairs, ScalarOutput, ScalarOutput>
{

    /**
     * Instance of {@link PearsonsCorrelation}.
     */

    private final PearsonsCorrelation correlation;

    /**
     * Message logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationPearsons.class);

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        try
        {
            DataFactory d = getDataFactory();
            MetadataFactory mF = d.getMetadataFactory();
            Slicer slicer = d.getSlicer();
            Metadata in = s.getMetadata();
            //Set the metadata explicitly since this class implements Collectable and getID() may be overridden
            MetricOutputMetadata meta = mF.getOutputMetadata(s.size(),
                                                             mF.getDimension(),
                                                             in.getDimension(),
                                                             MetricConstants.CORRELATION_PEARSONS,
                                                             MetricConstants.MAIN,
                                                             in.getIdentifier());
            double returnMe = correlation.correlation(slicer.getLeftSide(s), slicer.getRightSide(s));
            return getDataFactory().ofScalarOutput(returnMe, meta);
        }
        catch(Exception e)
        {
            LOGGER.error("While computing Pearson's correlation coefficient.", e);
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
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
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
        return MetricConstants.CORRELATION_PEARSONS;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class CorrelationPearsonsBuilder extends MetricBuilder<SingleValuedPairs, ScalarOutput>
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
     * @param builder the builder
     */

    protected CorrelationPearsons(final CorrelationPearsonsBuilder builder)
    {
        super(builder);
        correlation = new PearsonsCorrelation();
    }

}
