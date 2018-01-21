package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * Computes Pearson's product-moment correlation coefficient between the left and right sides of the {SingleValuedPairs}
 * input. Implements {@link Collectable} to avoid repeated calculations of derivative metrics, such as the
 * {@link CoefficientOfDetermination} when both appear in a {@link MetricCollection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class CorrelationPearsons extends OrdinaryScore<SingleValuedPairs, ScalarOutput>
implements Collectable<SingleValuedPairs, ScalarOutput, ScalarOutput>
{

    /**
     * Instance of {@link PearsonsCorrelation}.
     */

    private final PearsonsCorrelation correlation;

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        try
        {
            DataFactory d = getDataFactory();
            MetadataFactory mF = d.getMetadataFactory();
            Slicer slicer = d.getSlicer();
            Metadata in = s.getMetadata();
            //Set the metadata explicitly since this class implements Collectable and getID() may be overridden
            MetricOutputMetadata meta = mF.getOutputMetadata(s.getData().size(),
                                                             mF.getDimension(),
                                                             in.getDimension(),
                                                             MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                             MetricConstants.MAIN,
                                                             in.getIdentifier());
            double returnMe = correlation.correlation(slicer.getLeftSide(s), slicer.getRightSide(s));
            return getDataFactory().ofScalarOutput(returnMe, meta);
        }
        catch(Exception e)
        {
            throw new MetricCalculationException("While processing Pearson's correlation coefficient:", e);
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
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public ScalarOutput aggregate(ScalarOutput output)
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
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class CorrelationPearsonsBuilder extends OrdinaryScoreBuilder<SingleValuedPairs, ScalarOutput>
    {

        @Override
        public CorrelationPearsons build() throws MetricParameterException
        {
            return new CorrelationPearsons(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    protected CorrelationPearsons(final CorrelationPearsonsBuilder builder) throws MetricParameterException
    {
        super(builder);
        correlation = new PearsonsCorrelation();
    }

}
