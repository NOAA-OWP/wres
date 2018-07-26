package wres.datamodel.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.MetricConstants;

/**
 * A factory class for producing metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetadataFactory
{

    /**
     * Build a {@link Metadata} object with a default {@link Dimension}.
     * 
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata()
    {
        return MetadataFactory.getMetadata( getDimension() );
    }

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension}.
     * 
     * @param dim the dimension
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata( final Dimension dim )
    {
        return MetadataFactory.getMetadata( dim, null, null );
    }

    /**
     * Build a {@link Metadata} object with a prescribed {@link Dimension} and an optional {@link DatasetIdentifier}.
     * 
     * @param dim the dimension
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata( final Dimension dim, final DatasetIdentifier identifier )
    {
        return MetadataFactory.getMetadata( dim, identifier, null );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link Dimension}.
     * 
     * @param input the source metadata
     * @param dim the new dimension
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata( final Metadata input, final Dimension dim )
    {
        return MetadataFactory.getMetadata( dim, input.getIdentifier(), input.getTimeWindow() );
    }

    /**
     * Builds a {@link Metadata} from a prescribed input source and a new {@link TimeWindow}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata( final Metadata input, final TimeWindow timeWindow )
    {
        return MetadataFactory.getMetadata( input.getDimension(), input.getIdentifier(), timeWindow );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier getDatasetIdentifier( final Location geospatialID, final String variableID )
    {
        return MetadataFactory.getDatasetIdentifier( geospatialID, variableID, null, null );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier getDatasetIdentifier( final Location geospatialID,
                                                          final String variableID,
                                                          final String scenarioID )
    {
        return MetadataFactory.getDatasetIdentifier( geospatialID, variableID, scenarioID, null );
    }

    /**
     * Returns a new dataset identifier with an override for the {@link DatasetIdentifier#getScenarioIDForBaseline()}.
     * 
     * @param identifier the dataset identifier
     * @param baselineScenarioID a scenario identifier for a baseline dataset
     * @return a dataset identifier
     */

    public static DatasetIdentifier getDatasetIdentifier( DatasetIdentifier identifier, String baselineScenarioID )
    {
        return MetadataFactory.getDatasetIdentifier( identifier.getGeospatialID(),
                                                     identifier.getVariableID(),
                                                     identifier.getScenarioID(),
                                                     baselineScenarioID );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and a {@link MetricConstants} identifier for the metric.
     * 
     * @param sampleSize the sample size
     * @param outputDim the dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final int sampleSize,
                                                          final Dimension outputDim,
                                                          final Dimension inputDim,
                                                          final MetricConstants metricID )
    {
        return MetadataFactory.getOutputMetadata( sampleSize,
                                                  outputDim,
                                                  inputDim,
                                                  metricID,
                                                  MetricConstants.MAIN,
                                                  null,
                                                  null );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, and {@link MetricConstants} identifiers for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final int sampleSize,
                                                          final Dimension outputDim,
                                                          final Dimension inputDim,
                                                          final MetricConstants metricID,
                                                          final MetricConstants componentID )
    {
        return MetadataFactory.getOutputMetadata( sampleSize, outputDim, inputDim, metricID, componentID, null, null );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed source of {@link Metadata} whose parameters are
     * copied, together with a sample size, a {@link Dimension} for the output, and {@link MetricConstants} identifiers
     * for the metric and the metric component, respectively.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param metadata the source metadata
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final int sampleSize,
                                                          final Dimension outputDim,
                                                          final Metadata metadata,
                                                          final MetricConstants metricID,
                                                          final MetricConstants componentID )
    {
        Objects.requireNonNull( metadata,
                                "Specify a non-null source of input metadata from which to build the output metadata." );
        return MetadataFactory.getOutputMetadata( sampleSize,
                                                  outputDim,
                                                  metadata.getDimension(),
                                                  metricID,
                                                  componentID,
                                                  metadata.getIdentifier(),
                                                  metadata.getTimeWindow() );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, {@link MetricConstants} identifiers for the metric and the metric component, respectively, and an
     * optional {@link DatasetIdentifier} identifier.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final int sampleSize,
                                                          final Dimension outputDim,
                                                          final Dimension inputDim,
                                                          final MetricConstants metricID,
                                                          final MetricConstants componentID,
                                                          final DatasetIdentifier identifier )
    {
        return MetadataFactory.getOutputMetadata( sampleSize,
                                                  outputDim,
                                                  inputDim,
                                                  metricID,
                                                  componentID,
                                                  identifier,
                                                  null );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the metric component identifier.
     * 
     * @param source the input source
     * @param componentID the metric component identifier or decomposition template
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final MetricOutputMetadata source,
                                                          final MetricConstants componentID )
    {
        return MetadataFactory.getOutputMetadata( source.getSampleSize(),
                                                  source.getDimension(),
                                                  source.getInputDimension(),
                                                  source.getMetricID(),
                                                  componentID,
                                                  source.getIdentifier(),
                                                  source.getTimeWindow() );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the {@link TimeWindow}.
     * 
     * @param source the input source
     * @param timeWindow the time window
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final MetricOutputMetadata source,
                                                          final TimeWindow timeWindow )
    {
        return MetadataFactory.getOutputMetadata( source.getSampleSize(),
                                                  source.getDimension(),
                                                  source.getInputDimension(),
                                                  source.getMetricID(),
                                                  source.getMetricComponentID(),
                                                  source.getIdentifier(),
                                                  timeWindow );
    }

    /**
     * Builds a {@link MetricOutputMetadata} with an input source and an override for the sample size.
     * 
     * @param source the input source
     * @param sampleSize the sample size
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final MetricOutputMetadata source,
                                                          final int sampleSize )
    {
        return MetadataFactory.getOutputMetadata( sampleSize,
                                                  source.getDimension(),
                                                  source.getInputDimension(),
                                                  source.getMetricID(),
                                                  source.getMetricComponentID(),
                                                  source.getIdentifier(),
                                                  source.getTimeWindow() );
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

    public static Metadata getMetadata( final Dimension dim, final DatasetIdentifier identifier, TimeWindow timeWindow )
    {
        return Metadata.of( dim, identifier, timeWindow );
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, a {@link Dimension} for the output
     * and the input, {@link MetricConstants} identifiers for the metric and the metric component, respectively, and an
     * optional {@link DatasetIdentifier} identifier and {@link TimeWindow}.
     * 
     * @param sampleSize the sample size
     * @param outputDim the output dimension
     * @param inputDim the input dimension
     * @param metricID the metric identifier
     * @param componentID the metric component identifier or decomposition template
     * @param identifier an optional dataset identifier (may be null)
     * @param timeWindow an optional time window (may be null)
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getOutputMetadata( final int sampleSize,
                                                          final Dimension outputDim,
                                                          final Dimension inputDim,
                                                          final MetricConstants metricID,
                                                          final MetricConstants componentID,
                                                          final DatasetIdentifier identifier,
                                                          final TimeWindow timeWindow )
    {
        return MetricOutputMetadata.of( sampleSize,
                                        outputDim,
                                        inputDim,
                                        metricID,
                                        componentID,
                                        identifier,
                                        timeWindow );
    }

    /**
     * Returns a dataset identifier.
     * 
     * @param geospatialID an optional geospatial identifier (may be null)
     * @param variableID an optional variable identifier (may be null)
     * @param scenarioID an optional scenario identifier (may be null)
     * @param baselineScenarioID an optional scenario identifier for a baseline dataset (may be null)
     * @return a dataset identifier
     */

    public static DatasetIdentifier getDatasetIdentifier( final Location geospatialID,
                                                          final String variableID,
                                                          final String scenarioID,
                                                          final String baselineScenarioID )
    {
        return DatasetIdentifier.of( geospatialID, variableID, scenarioID, baselineScenarioID );
    }

    /**
     * Returns a location.
     *
     * @param vectorIdentifier An optional identifier for vector locations (may be null)
     * @param locationName An optional name for a location (may be null)
     * @param longitude An optional longitudinal coordinate for a location (may be null)
     * @param latitude An optional latitudinal coordinate for a location (may be null)
     * @param gageId And optional identifier for a gage (may be null)
     * @return a location
     */

    public static Location getLocation( final Long vectorIdentifier,
                                        final String locationName,
                                        final Float longitude,
                                        final Float latitude,
                                        final String gageId )
    {
        return Location.of( vectorIdentifier, locationName, longitude, latitude, gageId );
    }

    /**
     * Returns a location.
     * 
     * @param longitude An optional longitudinal coordinate for a location (may be null)
     * @param latitude An optional latitudinal coordinate for a location (may be null)
     * @return a location
     */

    public static Location getLocation( final Float longitude, final Float latitude )
    {
        return MetadataFactory.getLocation( null, null, longitude, latitude, null );
    }

    /**
     * Returns a location.
     * 
     * @param locationName An optional name for a location (may be null)
     * @return a location
     */

    public static Location getLocation( final String locationName )
    {
        return MetadataFactory.getLocation( null, locationName, null, null, null );
    }

    /**
     * Returns a location
     * @param vectorIdentifier An optional vector identifier for a location (may be null)
     * @return A location
     */
    public static Location getLocation( final Long vectorIdentifier )
    {
        return MetadataFactory.getLocation( vectorIdentifier, null, null, null, null );
    }

    /**
     * Returns a {@link Dimension} that is nominally dimensionless.
     * 
     * @return a {@link Dimension}
     */

    public static Dimension getDimension()
    {
        return Dimension.of( "DIMENSIONLESS" );
    }

    /**
     * Returns a {@link Dimension} with a named dimension and {@link Dimension#hasDimension()} that returns false if the
     * dimension is "DIMENSIONLESS", true otherwise.
     * 
     * @param dimension the dimension string
     * @return a {@link Dimension}
     * @throws MetadataException if the input string is null
     */

    public static Dimension getDimension( final String dimension )
    {
        return Dimension.of( dimension );
    }

    /**
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link TimeWindow}, otherwise an exception is thrown. See also {@link TimeWindow#unionOf(List)}.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws MetadataException if the input is invalid
     */

    public static Metadata unionOf( List<Metadata> input )
    {
        String nulLString = "Cannot find the union of null metadata.";
        if ( Objects.isNull( input ) )
        {
            throw new MetadataException( nulLString );
        }
        if ( input.isEmpty() )
        {
            throw new MetadataException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();
        Metadata test = input.get( 0 );
        for ( Metadata next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetadataException( nulLString );
            }
            if ( !next.equalsWithoutTimeWindow( test ) )
            {
                throw new MetadataException( "Only the time window can differ when finding the union of metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }
        if ( !unionWindow.isEmpty() )
        {
            test = getMetadata( test, TimeWindow.unionOf( unionWindow ) );
        }
        return test;
    }

    /**
     * No argument constructor.
     */

    private MetadataFactory()
    {
    }

}
