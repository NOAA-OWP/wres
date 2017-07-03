package wres.datamodel.metric;

/**
 * A factory class for producing metadata.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetadataFactory
{

    /**
     * Build a {@link Metadata} object with a sample size and a default {@link Dimension}.
     * 
     * @param sampleSize the sample size
     * @return a {@link Metadata} object
     */

    Metadata getMetadata(final int sampleSize);

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension} and identifiers for the
     * input data and, possible, a baseline (may be null).
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param id an identifier associated with the metric data (may be null)
     * @return a {@link Metadata} object
     */

    Metadata getMetadata(final int sampleSize, final Dimension dim, final String id);

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, {@link Dimension}, and identifiers
     * for the metric and the metric component, as well as the data and baseline data (may be null).
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param metricID the metric identifier
     * @param mainID an identifier associated with the metric data (may be null)
     * @param baseID an identifier associated with the baseline metric data (may be null)
     * @param componentID the metric component identifier
     * @return a {@link MetricOutputMetadata} object
     */

    MetricOutputMetadata getMetadata(final int sampleSize,
                                     final Dimension dim,
                                     final MetricConstants metricID,
                                     final MetricConstants componentID,
                                     final String mainID,
                                     final String baseID);

    /**
     * Returns a {@link Dimension} that is nominally dimensionless.
     * 
     * @return a {@link Dimension}
     */

    Dimension getDimension();

    /**
     * Returns a {@link Dimension} with a named dimension and {@link Dimension#hasDimension()} that returns false if the
     * dimension is "DIMENSIONLESS", true otherwise.
     * 
     * @param dimension the dimension string
     * @return a {@link Dimension}
     */

    Dimension getDimension(final String dimension);

    /**
     * Returns a metric name for a prescribed metric identifier from this class.
     * 
     * @param identifier the metric identifier
     * @return a metric name for the input identifier
     */

    String getMetricName(final MetricConstants identifier);

    /**
     * Returns the name associated with a prescribed metric component from this class, such as a score component.
     * 
     * @param identifier the metric component identifier
     * @return a metric component name for the input identifier
     */

    String getMetricComponentName(final MetricConstants identifier);

}
