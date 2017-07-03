package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * A generic implementation of an error score that applies a {@link DoubleErrorFunction} to each pair within a
 * {@link SingleValuedPairs} and returns the average error across those pairs.
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
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Metadata
        final Metadata mainMeta = s.getMetadata();
        String baseID = null;
        Dimension d = null;
        if(hasRealUnits())
        {
            d = mainMeta.getDimension();
        }
        if(s.hasBaseline() && isSkillScore())
        {
            baseID = s.getMetadataForBaseline().getID();
        }
        final MetricOutputMetadata metOut =
                                          getOutputFactory().getMetadataFactory().getMetadata(mainMeta.getSampleSize(),
                                                                                              d,
                                                                                              getID(),
                                                                                              MetricConstants.MAIN,
                                                                                              mainMeta.getID(),
                                                                                              baseID);
        //Compute the atomic errors in a stream
        return getOutputFactory().ofScalarOutput(s.getData().stream().mapToDouble(f).average().getAsDouble(), metOut);
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    protected DoubleErrorScore(final DoubleErrorScoreBuilder<S> b)
    {
        super(b.outputFactory);
        Objects.requireNonNull(b.f, "Specify a non-null function from which to construct the metric.");
        this.f = b.f;
    }

}
