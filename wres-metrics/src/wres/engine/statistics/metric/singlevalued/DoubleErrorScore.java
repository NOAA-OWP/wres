package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.Score;

/**
 * A generic implementation of an error score that cannot be decomposed. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class DoubleErrorScore<S extends SingleValuedPairs> extends Metric<S, ScalarOutput> implements Score
{
    /**
     * The error function.
     */

    DoubleErrorFunction f;

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static abstract class DoubleErrorScoreBuilder<S extends SingleValuedPairs>
    extends
        MetricBuilder<S, ScalarOutput>
    {

        /**
         * The error function associated with the score.
         */
        private DoubleErrorFunction f;

        /**
         * Sets the error function.
         * 
         * @param f the error function
         * @return the metric builder
         */

        public DoubleErrorScoreBuilder<S> setErrorFunction(final DoubleErrorFunction f)
        {
            this.f = f;
            return this;
        }

    }

    @Override
    public ScalarOutput apply(final S s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        Objects.requireNonNull(f, "Override or specify a non-null error function for the '" + toString() + "'.");
        
        //Metadata
        DatasetIdentifier id = null;
        if(s.hasBaseline() && s.getMetadataForBaseline().hasIdentifier())
        {
            id = s.getMetadataForBaseline().getIdentifier();
        }
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, id);
        //Compute the atomic errors in a stream
        return getDataFactory().ofScalarOutput(s.getData().stream().mapToDouble(f).average().getAsDouble(), metOut);
    }
    
    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }    
    
    @Override
    public boolean isDecomposable()
    {
        return false;
    }    
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    protected DoubleErrorScore(final DoubleErrorScoreBuilder<S> builder) throws MetricParameterException
    {
        super(builder);
        this.f = builder.f;
    }

}
