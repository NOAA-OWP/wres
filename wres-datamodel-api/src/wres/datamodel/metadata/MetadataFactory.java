package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.time.TimeWindow;

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
     * Build a {@link Metadata} object with a default {@link Dimension}.
     * 
     * @return a {@link Metadata} object
     */

    default Metadata getMetadata()
    {
        return getMetadata(getDimension());
    }

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension}.
     * 
     * @param dim the dimension
     * @return a {@link Metadata} object
     */

    default Metadata getMetadata(final Dimension dim)
    {
        return getMetadata(dim, null, null);
    }

    /**
     * Build a {@link Metadata} object with a prescribed {@link Dimension} and an optional {@link DatasetIdentifier}.
     * 
     * @param dim the dimension
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link Metadata} object
     */

    default Metadata getMetadata(final Dimension dim, final DatasetIdentifier identifier)
    {
        return getMetadata(dim, identifier, null);
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link Dimension}.
     * 
     * @param input the source metadata
     * @param dim the new dimension
     * @return a {@link Metadata} object
     */

    default Metadata getMetadata(final Metadata input, final Dimension dim)
    {
        return getMetadata(dim, input.getIdentifier(), input.getTimeWindow());
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @return a dataset identifier
     */

    default DatasetIdentifier getDatasetIdentifier(final String geospatialID, final String variableID)
    {
        return getDatasetIdentifier(geospatialID, variableID, null, null);
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier (may be null)
     * @return a dataset identifier
     */

    default DatasetIdentifier getDatasetIdentifier(final String geospatialID,
                                                   final String variableID,
                                                   final String scenarioID)
    {
        return getDatasetIdentifier(geospatialID, variableID, scenarioID, null);
    }

    /**
     * Returns a new dataset identifier with an override for the {@link DatasetIdentifier#getScenarioIDForBaseline()}.
     * 
     * @param identifier the dataset identifier
     * @param baselineScenarioID a scenario identifier for a baseline dataset
     * @return a dataset identifier
     */

    default DatasetIdentifier getDatasetIdentifier(DatasetIdentifier identifier, String baselineScenarioID)
    {
        return getDatasetIdentifier(identifier.getGeospatialID(),
                                    identifier.getVariableID(),
                                    identifier.getScenarioID(),
                                    baselineScenarioID);
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and a {@link MetricConstants} identifier for the metric.
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @return a {@link MetricOutputMetadata} object
     */

    default MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                   final Dimension dim,
                                                   final Dimension inputDim,
                                                   final MetricConstants metricID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, MetricConstants.MAIN, null, null);
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and {@link MetricConstants} identifiers for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    default MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                   final Dimension dim,
                                                   final Dimension inputDim,
                                                   final MetricConstants metricID,
                                                   final MetricConstants componentID)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, componentID, null, null);
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed source of {@link Metadata} whose parameters are
     * copied, together with a sample size, a {@link Dimension} for the output, and {@link MetricConstants} identifiers
     * for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param metadata the source metadata
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    default MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                   final Dimension dim,
                                                   final Metadata metadata,
                                                   final MetricConstants metricID,
                                                   final MetricConstants componentID)
    {
        Objects.requireNonNull(metadata,
                               "Specify a non-null source of input metadata from which to build the output metadata.");
        return getOutputMetadata(sampleSize,
                                 dim,
                                 metadata.getDimension(),
                                 metricID,
                                 componentID,
                                 metadata.getIdentifier(),
                                 metadata.getTimeWindow());
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, {@link MetricConstants} identifiers for the metric and the metric component, respectively, and an
     * optional {@link DatasetIdentifier} identifier.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    default MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                   final Dimension dim,
                                                   final Dimension inputDim,
                                                   final MetricConstants metricID,
                                                   final MetricConstants componentID,
                                                   final DatasetIdentifier identifier)
    {
        return getOutputMetadata(sampleSize, dim, inputDim, metricID, componentID, identifier, null);
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the metric component identifier.
     * 
     * @param source the input source
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    default MetricOutputMetadata getOutputMetadata(final MetricOutputMetadata source, final MetricConstants componentID)
    {
        return getOutputMetadata(source.getSampleSize(),
                                 source.getDimension(),
                                 source.getInputDimension(),
                                 source.getMetricID(),
                                 componentID,
                                 source.getIdentifier(),
                                 source.getTimeWindow());
    }

    /**
     * Build a {@link Metadata} object with a prescribed {@link Dimension} and an optional {@link DatasetIdentifier} and
     * {@link TimeWindow}.
     * 
     * @param dim the dimension
     * @param identifier an optional dataset identifier (may be null)
     * @param timeWindow an optional time window (may be null)
     * @return a {@link Metadata} object
     */

    Metadata getMetadata(final Dimension dim, final DatasetIdentifier identifier, TimeWindow timeWindow);

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, {@link MetricConstants} identifiers for the metric and the metric component, respectively, and an
     * optional {@link DatasetIdentifier} identifier and {@link TimeWindow}.
     * 
     * @param sampleSize the sample size
     * @param dim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param identifier an optional dataset identifier (may be null)
     * @param timeWindow an optional time window (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                           final Dimension dim,
                                           final Dimension inputDim,
                                           final MetricConstants metricID,
                                           final MetricConstants componentID,
                                           final DatasetIdentifier identifier,
                                           final TimeWindow timeWindow);

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier (may be null)
     * @param baselineScenarioID an optional scenario identifier for a baseline dataset (may be null)
     * @return a dataset identifier
     */

    DatasetIdentifier getDatasetIdentifier(final String geospatialID,
                                           final String variableID,
                                           final String scenarioID,
                                           final String baselineScenarioID);

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

}
