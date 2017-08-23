package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;

/**
 * A generic implementation of an error score without decomposition. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
abstract class DoubleErrorScore<S extends SingleValuedPairs> extends Metric<S, ScalarOutput> implements Score
{
    /**
     * The error function.
     */

    DoubleErrorFunction f;

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static abstract class DoubleErrorScoreBuilder<S extends SingleValuedPairs>
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
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
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

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    protected DoubleErrorScore(final DoubleErrorScoreBuilder<S> builder)
    {
        super(builder);
        this.f = builder.f;
    }

}
