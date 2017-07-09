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
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension}.
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @return a {@link Metadata} object
     */

    Metadata getMetadata(final int sampleSize, final Dimension dim);

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension} and several optional
     * identifiers.
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param geospatialID an optional geospatial (e.g. location) identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier associated with the metric data (may be null)
     * @return a {@link Metadata} object
     */

    Metadata getMetadata(final int sampleSize,
                         final Dimension dim,
                         final String geospatialID,
                         String variableID,
                         String scenarioID);

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and an identifier for the metric.
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @return a {@link MetricOutputMetadata} object
     */

    MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                           final Dimension dim,
                                           final Dimension inputDim,
                                           final MetricConstants metricID);

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and identifiers for the metric and the metric component.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                           final Dimension dim,
                                           final Dimension inputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID);

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, identifiers for the metric and the metric component, and several optional identifiers.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param geospatialID an optional geospatial (e.g. location) identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier associated with the metric data (may be null)
     * @param baseScenarioID an optional scenario identifier associated with the baseline metric data (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                           final Dimension dim,
                                           final Dimension inputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID,
                                           final String geospatialID,
                                           final String variableID,
                                           final String scenarioID,
                                           final String baseScenarioID);

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
     * Returns the fully qualified metric name for a prescribed metric identifier from this class. See
     * {@link #getMetricShortName(MetricConstants)}.
     * 
     * @param identifier the metric identifier
     * @return a long metric name for the input identifier
     */

    String getMetricName(final MetricConstants identifier);

    /**
     * Returns an abbreviated metric name for a prescribed metric identifier from this class. See
     * {@link #getMetricName(MetricConstants)}.
     * 
     * @param identifier the metric identifier
     * @return an abbreviated metric name for the input identifier
     */

    String getMetricShortName(final MetricConstants identifier);

    /**
     * Returns the name associated with a prescribed metric component from this class, such as a score component.
     * 
     * @param identifier the metric component identifier
     * @return a metric component name for the input identifier
     */

    String getMetricComponentName(final MetricConstants identifier);

}
