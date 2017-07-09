package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.function.Function;

import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;

/**
 * <p>
 * Abstract base class for an immutable metric. The metric calculation is implemented in {@link #apply(MetricInput)}.
 * The metric may operate on paired or unpaired inputs, and inputs that comprise one or more individual datasets. The
 * structure of the input and output is prescribed by a particular subclass.
 * </p>
 * <p>
 * A metric cannot conduct any pre-processing of the input, such as rescaling, changing measurement units, conditioning,
 * or removing missing values (except for missing ensemble members whose treatment may be metric-specific and whose
 * inclusion may be important for recovering ensemble traces).
 * </p>
 * <p>
 * In order to build a metric, implement the inner class {@link MetricBuilder} and validate the parameters in a hidden
 * constructor.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class Metric<S extends MetricInput<?>, T extends MetricOutput<?>> implements Function<S, T>
{

    /**
     * Instance of a {@link MetricOutputFactory} for constructing a {@link MetricOutput}.
     */

    private final MetricOutputFactory outputFactory;

    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param s the input
     * @return the output
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    public abstract T apply(S s);

    /**
     * Returns a unique identifier for the metric from {@link MetricConstants}.
     * 
     * @return a unique identifier
     */

    public abstract MetricConstants getID();

    /**
     * Returns true if the metric generates outputs that are dimensioned in real units, false if the outputs are in
     * statistical or probabilistic units.
     * 
     * @return true if the outputs are dimensioned in real units, false otherwise
     */

    public abstract boolean hasRealUnits();

    /**
     * Returns the unique name of the metric. See also {@link #getID()} and
     * {@link MetadataFactory#getMetricName(MetricConstants)}.
     * 
     * @return the unique metric name
     */

    public String getName()
    {
        return getOutputFactory().getMetadataFactory().getMetricName(getID());
    }

    /**
     * Returns {@link #getName()}
     * 
     * @return a string representation of the metric.
     */

    @Override
    public String toString()
    {
        return getName();
    }

    /**
     * Returns true when the input is a {@link Metric} and the metrics have equivalent names, i.e. {@link #getName()}
     * returns an equivalent string.
     * 
     * @param o the object to test for equality with the current object
     * @return true if the input is a metric and has an equivalent name to the current metric, false otherwise
     */
    public boolean nameEquals(final Object o)
    {
        return o != null && o instanceof Metric && ((Metric<?, ?>)o).getName().equals(getName());
    }

    /**
     * An abstract builder to build a {@link Metric}. Implement this interface when building a {@link Metric}, and hide
     * the constructor. Add setters to set the parameters of the metric, as required, prior to building. For thread
     * safety, validate the parameters using the hidden constructor of the {@link Metric}, i.e. do not validate the
     * parameters in the {@link MetricBuilder} before construction.
     */

    protected static abstract class MetricBuilder<P extends MetricInput<?>, Q extends MetricOutput<?>>
    {

        protected MetricOutputFactory outputFactory;

        /**
         * Build the {@link Metric}.
         * 
         * @return a {@link Metric}
         */

        protected abstract Metric<P, Q> build();

        /**
         * Sets the {@link MetricOutputFactory} for constructing a {@link MetricOutput}.
         * 
         * @param outputFactory the {@link MetricOutputFactory}
         * @return the builder
         */

        protected MetricBuilder<P, Q> setOutputFactory(final MetricOutputFactory outputFactory)
        {
            this.outputFactory = outputFactory;
            return this;
        }

    }

    /**
     * Returns a {@link MetricOutputFactory} for constructing a {@link MetricOutput}.
     * 
     * @return a {@link MetricOutputFactory}
     */

    protected MetricOutputFactory getOutputFactory()
    {
        return outputFactory;
    }

    /**
     * Returns the {@link MetricOutputMetadata} using a prescribed {@link MetricInput} and the current {@link Metric} to
     * compose, along with an explicit component identifier or decomposition template and an identifier for the
     * baseline, where applicable
     * 
     * @param input the metric input
     * @param sampleSize the sample size
     * @param componentID the component identifier or metric decomposition template
     * @param baselineID the baseline identifier or null
     * @return the metadata
     */

    protected MetricOutputMetadata getMetadata(final MetricInput<?> input,
                                               final int sampleSize,
                                               final MetricConstants componentID,
                                               final String baselineID)
    {
        final Metadata metIn = input.getMetadata();
        Dimension outputDim = null;
        //Dimensioned?
        if(hasRealUnits())
        {
            outputDim = metIn.getDimension();
        }
        else
        {
            outputDim = outputFactory.getMetadataFactory().getDimension();
        }
        return outputFactory.getMetadataFactory().getOutputMetadata(sampleSize,
                                                                    outputDim,
                                                                    metIn.getDimension(),
                                                                    getID(),
                                                                    componentID,
                                                                    metIn.getGeospatialID(),
                                                                    metIn.getVariableID(),
                                                                    metIn.getScenarioID(),
                                                                    baselineID);
    }

    /**
     * Construct a {@link Metric} with a {@link MetricOutputFactory}.
     * 
     * @param outputFactory the {@link MetricOutputFactory}.
     */

    protected Metric(final MetricOutputFactory outputFactory)
    {
        if(Objects.isNull(outputFactory))
        {
            throw new UnsupportedOperationException("Cannot construct the metric without a metric output factory.");
        }
        this.outputFactory = outputFactory;
    }

}
