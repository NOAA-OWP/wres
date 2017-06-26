package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * A generic implementation of an error score that applies a {@link DoubleErrorFunction} to each pair within a
 * {@link SingleValuedPairs} and returns the average error across those pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class DoubleErrorScore<S extends SingleValuedPairs, T extends ScalarOutput> extends Metric<S, T>
implements Score
{
    /**
     * The error function.
     */

    DoubleErrorFunction f;

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static abstract class DoubleErrorScoreBuilder<S extends SingleValuedPairs, T extends ScalarOutput>
    implements MetricBuilder<S, T>
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

        public DoubleErrorScoreBuilder<S, T> setErrorFunction(final DoubleErrorFunction f)
        {
            this.f = f;
            return this;
        }

    }

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Metadata
        final Metadata metIn = s.getMetadata();
        Dimension d = null;
        if(hasRealUnits())
        {
            d = metIn.getDimension();
        }
        final MetricOutputMetadata metOut = MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                                        d,
                                                                        getID(),
                                                                        MetricConstants.MAIN,
                                                                        metIn.getID(),
                                                                        metIn.getIDForBaseline());
        //Compute the atomic errors in a stream
        return MetricOutputFactory.ofScalarExtendsMetricOutput(s.getData(0)
                                                                .stream()
                                                                .mapToDouble(f)
                                                                .average()
                                                                .getAsDouble(),
                                                               metOut);
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    protected DoubleErrorScore(final DoubleErrorScoreBuilder<S, T> b)
    {
        Objects.requireNonNull(b.f, "Specify a non-null function from which to construct the metric.");
        this.f = b.f;
    }

}
