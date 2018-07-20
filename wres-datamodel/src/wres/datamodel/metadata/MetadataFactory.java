package wres.datamodel.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.Location;
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
        return new MetadataImpl( dim, identifier, timeWindow );
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
        class MetricOutputMetadataImpl extends MetadataImpl implements MetricOutputMetadata
        {

            private MetricOutputMetadataImpl()
            {
                super( outputDim, identifier, timeWindow );
            }

            @Override
            public MetricConstants getMetricID()
            {
                return metricID;
            }

            @Override
            public MetricConstants getMetricComponentID()
            {
                return componentID;
            }

            @Override
            public Dimension getInputDimension()
            {
                return inputDim;
            }

            @Override
            public int getSampleSize()
            {
                return sampleSize;
            }

            @Override
            public boolean minimumEquals( final MetricOutputMetadata o )
            {
                return o.getMetricID() == getMetricID()
                       && o.getMetricComponentID() == getMetricComponentID()
                       && o.getDimension().equals( getDimension() )
                       && o.getInputDimension().equals( getInputDimension() );
            }

            @Override
            public boolean equals( final Object o )
            {
                if ( ! ( o instanceof MetricOutputMetadata ) )
                {
                    return false;
                }
                final MetricOutputMetadata p = ( (MetricOutputMetadata) o );
                boolean returnMe = super.equals( o ) && p.getSampleSize() == getSampleSize()
                                   && p.getInputDimension().equals( getInputDimension() );
                return returnMe && p.getMetricID() == getMetricID()
                       && p.getMetricComponentID() == getMetricComponentID();
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(),
                                     getSampleSize(),
                                     getMetricID(),
                                     getMetricComponentID(),
                                     getInputDimension() );
            }

            @Override
            public String toString()
            {
                String start = super.toString();
                start = start.substring( 0, start.length() - 1 ); // Remove bookend char, ']'
                final StringBuilder b = new StringBuilder( start );
                b.append( "," )
                 .append( inputDim )
                 .append( "," )
                 .append( sampleSize )
                 .append( "," )
                 .append( metricID )
                 .append( "," )
                 .append( componentID )
                 .append( "]" );
                return b.toString();
            }

        }

        return new MetricOutputMetadataImpl();
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
        return new DatasetIdentifierImpl( geospatialID, variableID, scenarioID, baselineScenarioID );
    }

    /**
     * Returns a location
     *
     * @param vectorIdentifier An optional identifier for vector locations (may be null)
     * @param locationName An optional name for a location (may be null)
     * @param longitude An optional longitudinal coordinate for a location (may be null)
     * @param latitude An optional latitudinal coordinate for a location (may be null)
     * @param gageId And optional identifier for a gage (may be null)
     * @return A location
     */
    public static Location getLocation( final Long vectorIdentifier,
                                        final String locationName,
                                        final Float longitude,
                                        final Float latitude,
                                        final String gageId )
    {
        return new LocationImpl( vectorIdentifier, locationName, longitude, latitude, gageId );
    }

    /**
     * Returns a location
     * @param longitude An optional longitudinal coordinate for a location (may be null)
     * @param latitude An optional latitudinal coordinate for a location (may be null)
     * @return A location
     */
    public static Location getLocation( final Float longitude, final Float latitude )
    {
        return MetadataFactory.getLocation( null, null, longitude, latitude, null );
    }

    /**
     * Returns a location
     * @param locationName An optional name for a location (may be null)
     * @return A location
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
        return getDimension( "DIMENSIONLESS" );
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
        class DimensionImpl implements Dimension
        {
            /**
             * The dimension.
             */
            private final String dimension;

            public DimensionImpl( final String dimension )
            {
                if ( Objects.isNull( dimension ) )
                {
                    throw new MetadataException( "Specify a non-null dimension string." );
                }
                this.dimension = dimension;
            }

            @Override
            public boolean hasDimension()
            {
                return !"DIMENSIONLESS".equals( dimension );
            }

            @Override
            public String getDimension()
            {
                return dimension;
            }

            @Override
            public boolean equals( final Object o )
            {
                return o instanceof Dimension && ( (Dimension) o ).hasDimension() == hasDimension()
                       && ( (Dimension) o ).getDimension().equals( getDimension() );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( hasDimension(), dimension );
            }

            @Override
            public String toString()
            {
                return getDimension();
            }

            @Override
            public int compareTo( Dimension o )
            {
                Objects.requireNonNull( o, "Specify a non-null dimension to compare with this dimension." );

                return dimension.compareTo( o.getDimension() );
            }
        }
        return new DimensionImpl( dimension );
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
     * Default implementation of {@link Metadata}.
     */

    private static class MetadataImpl implements Metadata
    {
        private final Dimension dim;
        private final DatasetIdentifier identifier;
        private final TimeWindow timeWindow;

        private MetadataImpl( final Dimension dim, final DatasetIdentifier identifier, final TimeWindow timeWindow )
        {
            if ( Objects.isNull( dim ) )
            {
                throw new MetadataException( "Specify a non-null dimension from which to construct the metadata." );
            }
            this.dim = dim;
            this.identifier = identifier;
            this.timeWindow = timeWindow;
        }

        @Override
        public Dimension getDimension()
        {
            return dim;
        }

        @Override
        public DatasetIdentifier getIdentifier()
        {
            return identifier;
        }

        @Override
        public TimeWindow getTimeWindow()
        {
            return timeWindow;
        }

        @Override
        public boolean equals( final Object o )
        {
            if ( ! ( o instanceof Metadata ) )
            {
                return false;
            }
            final Metadata p = (Metadata) o;
            boolean returnMe = equalsWithoutTimeWindow( p ) && hasTimeWindow() == p.hasTimeWindow();
            if ( hasTimeWindow() )
            {
                returnMe = returnMe && getTimeWindow().equals( p.getTimeWindow() );
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( getDimension(), hasIdentifier(), hasTimeWindow(), getIdentifier(), getTimeWindow() );
        }

        @Override
        public String toString()
        {
            final StringBuilder b = new StringBuilder();
            if ( hasIdentifier() )
            {
                String appendMe = identifier.toString();
                appendMe = appendMe.replaceAll( "]", "," );
                b.append( appendMe );
            }
            else
            {
                b.append( "[" );
            }
            if ( hasTimeWindow() )
            {
                b.append( timeWindow ).append( "," );
            }
            b.append( dim ).append( "]" );
            return b.toString();
        }

    }

    /**
     * Default implementation of {@link DatasetIdentifier}.
     */

    private static class DatasetIdentifierImpl implements DatasetIdentifier
    {

        final Location geospatialID;
        final String variableID;
        final String scenarioID;
        final String baselineScenarioID;

        private DatasetIdentifierImpl( final Location geospatialID,
                                       final String variableID,
                                       final String scenarioID,
                                       final String baselineScenarioID )
        {
            this.geospatialID = geospatialID;
            this.variableID = variableID;
            this.scenarioID = scenarioID;
            this.baselineScenarioID = baselineScenarioID;
        }

        @Override
        public Location getGeospatialID()
        {
            return geospatialID;
        }

        @Override
        public String getVariableID()
        {
            return variableID;
        }

        @Override
        public String getScenarioID()
        {
            return scenarioID;
        }

        @Override
        public String getScenarioIDForBaseline()
        {
            return baselineScenarioID;
        }

        @Override
        public String toString()
        {
            final StringJoiner b = new StringJoiner( ",", "[", "]" );
            if ( hasGeospatialID() )
            {
                b.add( getGeospatialID().toString() );
            }
            if ( hasVariableID() )
            {
                b.add( getVariableID() );
            }
            if ( hasScenarioID() )
            {
                b.add( getScenarioID() );
            }
            if ( hasScenarioIDForBaseline() )
            {
                b.add( getScenarioIDForBaseline() );
            }
            return b.toString();
        }

        @Override
        public boolean equals( final Object o )
        {
            if ( ! ( o instanceof DatasetIdentifier ) )
            {
                return false;
            }
            final DatasetIdentifier check = (DatasetIdentifier) o;
            boolean returnMe = hasGeospatialID() == check.hasGeospatialID()
                               && hasVariableID() == check.hasVariableID()
                               && hasScenarioID() == check.hasScenarioID()
                               && hasScenarioIDForBaseline() == check.hasScenarioIDForBaseline();
            if ( hasGeospatialID() )
            {
                returnMe = returnMe && getGeospatialID().equals( check.getGeospatialID() );
            }
            if ( hasVariableID() )
            {
                returnMe = returnMe && getVariableID().equals( check.getVariableID() );
            }
            if ( hasScenarioID() )
            {
                returnMe = returnMe && getScenarioID().equals( check.getScenarioID() );
            }
            if ( hasScenarioIDForBaseline() )
            {
                returnMe = returnMe && getScenarioIDForBaseline().equals( check.getScenarioIDForBaseline() );
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( getGeospatialID(), getVariableID(), getScenarioID(), getScenarioIDForBaseline() );
        }
    }

    /**
     * Default implementation of {@link Location}
     */

    private static class LocationImpl implements Location
    {
        private final Long vectorIdentifier;
        private final String locationName;
        private final Float longitude;
        private final Float latitude;
        private final String gageId;

        private LocationImpl( final Long vectorIdentifier,
                              final String locationName,
                              final Float longitude,
                              final Float latitude,
                              final String gageId )
        {
            this.vectorIdentifier = vectorIdentifier;
            this.locationName = locationName;
            this.longitude = longitude;
            this.latitude = latitude;
            this.gageId = gageId;
        }

        @Override
        public Long getVectorIdentifier()
        {
            return this.vectorIdentifier;
        }

        @Override
        public String getLocationName()
        {
            return this.locationName;
        }

        @Override
        public Float getLongitude()
        {
            return this.longitude;
        }

        @Override
        public Float getLatitude()
        {
            return this.latitude;
        }

        @Override
        public String getGageId()
        {
            return this.gageId;
        }

        @Override
        public String toString()
        {
            if ( this.hasLocationName() )
            {
                return this.locationName;
            }

            if ( this.hasGageId() )
            {
                return this.gageId;
            }

            if ( this.hasVectorIdentifier() )
            {
                return this.vectorIdentifier.toString();
            }

            if ( this.hasCoordinates() )
            {
                String coordinates = "" + Math.abs( this.getLongitude() );

                if ( this.getLongitude() < 0 )
                {
                    coordinates += "W";
                }
                else
                {
                    coordinates += "E";
                }

                coordinates += " " + Math.abs( this.getLatitude() );

                if ( this.getLatitude() < 0 )
                {
                    coordinates += "S";
                }
                else
                {
                    coordinates += "N";
                }

                return coordinates;
            }

            return "Unknown";
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof Location )
            {
                Location other = (Location) obj;

                boolean locationsEqual = true;
                boolean vectorIDsEqual = true;
                boolean gageIDsEqual = true;
                boolean coordinatesEqual = true;

                if ( this.hasLocationName() && other.hasLocationName() )
                {
                    locationsEqual = this.getLocationName().equalsIgnoreCase( other.getLocationName() );
                }
                else if ( this.hasLocationName() || other.hasLocationName() )
                {
                    return false;
                }

                if ( this.hasVectorIdentifier() && other.hasVectorIdentifier() )
                {
                    vectorIDsEqual = this.getVectorIdentifier().equals( other.getVectorIdentifier() );
                }
                else if ( this.hasVectorIdentifier() || other.hasVectorIdentifier() )
                {
                    return false;
                }

                if ( this.hasGageId() && other.hasGageId() )
                {
                    gageIDsEqual = this.getGageId().equalsIgnoreCase( other.getGageId() );
                }
                else if ( this.hasGageId() || other.hasGageId() )
                {
                    return false;
                }

                if ( this.hasCoordinates() && other.hasCoordinates() )
                {
                    coordinatesEqual = this.getLatitude().equals( other.getLatitude() ) &&
                                       this.getLongitude().equals( other.getLongitude() );
                }
                else if ( this.hasCoordinates() || other.hasCoordinates() )
                {
                    return false;
                }

                return locationsEqual && vectorIDsEqual && gageIDsEqual && coordinatesEqual;
            }

            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.locationName,
                                 this.gageId,
                                 this.vectorIdentifier,
                                 this.longitude,
                                 this.latitude );
        }
    }

    /**
     * No argument constructor.
     */

    private MetadataFactory()
    {
    }

}
