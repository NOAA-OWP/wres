package wres.datamodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataException;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;

/**
 * A factory class for constructing {@link Metadata} and associated objects.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum DefaultMetadataFactory implements MetadataFactory
{

    /**
     * Instance of the factory using singleton enum pattern for thread-safe, lazy, construction.
     */

    INSTANCE;

    /**
     * Returns an instance of a {@link MetadataFactory}.
     * 
     * @return a {@link MetadataFactory}
     */

    public static MetadataFactory getInstance()
    {
        return INSTANCE;
    }

    @Override
    public Metadata getMetadata(final Dimension dim, final DatasetIdentifier identifier, final TimeWindow timeWindow)
    {
        return new MetadataImpl(dim, identifier, timeWindow);
    }

    @Override
    public MetricOutputMetadata getOutputMetadata(final int sampleSize,
                                                  final Dimension outputDim,
                                                  final Dimension inputDim,
                                                  final MetricConstants metricID,
                                                  final MetricConstants componentID,
                                                  final DatasetIdentifier identifier,
                                                  final TimeWindow timeWindow)
    {
        class MetricOutputMetadataImpl extends MetadataImpl implements MetricOutputMetadata
        {

            private MetricOutputMetadataImpl()
            {
                super(outputDim, identifier, timeWindow);
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
            public boolean minimumEquals(final MetricOutputMetadata o)
            {
                return o.getMetricID() == getMetricID()
                       && o.getMetricComponentID() == getMetricComponentID()
                       && o.getDimension().equals( getDimension() )
                       && o.getInputDimension().equals( getInputDimension() );
            }

            @Override
            public boolean equals(final Object o)
            {
                if(!(o instanceof MetricOutputMetadata))
                {
                    return false;
                }
                final MetricOutputMetadata p = ((MetricOutputMetadata)o);
                boolean returnMe = super.equals(o) && p.getSampleSize() == getSampleSize()
                    && p.getInputDimension().equals(getInputDimension());
                return returnMe && p.getMetricID() == getMetricID()
                    && p.getMetricComponentID() == getMetricComponentID();
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(),
                                     sampleSize,
                                     getMetricID(),
                                     getMetricComponentID(),
                                     getInputDimension() );
            }

            @Override
            public String toString()
            {
                String start = super.toString();
                start = start.substring( 0, start.length() -1 ); // Remove bookend char, ']'
                final StringBuilder b = new StringBuilder(start);
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
    
    @Override
    public Metadata unionOf( List<Metadata> input )
    {
        String nulLString = "Cannot find the union of null metadata.";
        if( Objects.isNull( input ) )
        {
            throw new MetadataException( nulLString );
        }
        if( input.isEmpty() )
        {
            throw new MetadataException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();
        Metadata test = input.get( 0 );
        for( Metadata next: input )
        {
            if( Objects.isNull( next ) )
            {
                throw new MetadataException( nulLString );
            }
            if( ! next.equalsWithoutTimeWindow( test ) )
            {
                throw new MetadataException( "Only the time window can differ when finding the union of metadata." );
            }
            if( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }
        if( !unionWindow.isEmpty() )
        {
            test = getMetadata( test, TimeWindow.unionOf( unionWindow ) );
        }
        return test;
    }
    

    @Override
    public DatasetIdentifier getDatasetIdentifier(final Location geospatialID,
                                                  final String variableID,
                                                  final String scenarioID,
                                                  final String baselineScenarioID)
    {
        return new DatasetIdentifierImpl(geospatialID, variableID, scenarioID, baselineScenarioID);
    }

    @Override
    public Location getLocation( Long vectorIdentifier,
                                 String locationName,
                                 Float longitude,
                                 Float latitude,
                                 String gageId )
    {
        return new LocationImpl( vectorIdentifier, locationName, longitude, latitude, gageId );
    }

    @Override
    public Dimension getDimension()
    {
        return getDimension("DIMENSIONLESS");
    }

    @Override
    public Dimension getDimension(final String dimension)
    {
        class DimensionImpl implements Dimension
        {
            /**
             * The dimension.
             */
            private final String dimension;

            public DimensionImpl(final String dimension)
            {
                if( Objects.isNull( dimension ) )
                {
                    throw new MetadataException( "Specify a non-null dimension string." );
                }
                this.dimension = dimension;
            }

            @Override
            public boolean hasDimension()
            {
                return !"DIMENSIONLESS".equals(dimension);
            }

            @Override
            public String getDimension()
            {
                return dimension;
            }

            @Override
            public boolean equals(final Object o)
            {
                return o instanceof Dimension && ((Dimension)o).hasDimension() == hasDimension()
                    && ((Dimension)o).getDimension().equals(getDimension());
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
        return new DimensionImpl(dimension);
    }

    /**
     * Default implementation of {@link Metadata}.
     */

    private class MetadataImpl implements Metadata
    {
        private final Dimension dim;
        private final DatasetIdentifier identifier;
        private final TimeWindow timeWindow;
        
        private MetadataImpl(final Dimension dim, final DatasetIdentifier identifier, final TimeWindow timeWindow)
        {
            if( Objects.isNull( dim ) )
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
        public boolean equals(final Object o)
        {
            if(!(o instanceof Metadata))
            {
                return false;
            }
            final Metadata p = (Metadata)o;
            boolean returnMe = equalsWithoutTimeWindow( p ) && hasTimeWindow() == p.hasTimeWindow();
            if(hasTimeWindow())
            {
                returnMe = returnMe && getTimeWindow().equals(p.getTimeWindow());
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
            if(hasIdentifier())
            {
                String appendMe = identifier.toString();
                appendMe = appendMe.replaceAll("]", ",");
                b.append(appendMe);
            }
            else
            {
                b.append("[");
            }
            if(hasTimeWindow())
            {
                b.append(timeWindow).append(",");
            }
            b.append(dim).append("]");
            return b.toString();
        }                
        
    }

    /**
     * Default implementation of {@link DatasetIdentifier}.
     */

    private class DatasetIdentifierImpl implements DatasetIdentifier
    {

        final Location geospatialID;
        final String variableID;
        final String scenarioID;
        final String baselineScenarioID;

        private DatasetIdentifierImpl(final Location geospatialID,
                                      final String variableID,
                                      final String scenarioID,
                                      final String baselineScenarioID)
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
            final StringJoiner b = new StringJoiner(",", "[", "]");
            if(hasGeospatialID())
            {
                b.add(getGeospatialID().toString());
            }
            if(hasVariableID())
            {
                b.add(getVariableID());
            }
            if(hasScenarioID())
            {
                b.add(getScenarioID());
            }
            if(hasScenarioIDForBaseline())
            {
                b.add(getScenarioIDForBaseline());
            }
            return b.toString();
        }

        @Override
        public boolean equals(final Object o)
        {
            if(!(o instanceof DatasetIdentifier))
            {
                return false;
            }
            final DatasetIdentifier check = (DatasetIdentifier)o;
            boolean returnMe = hasGeospatialID() == check.hasGeospatialID()
                               && hasVariableID() == check.hasVariableID()
                               && hasScenarioID() == check.hasScenarioID()
                               && hasScenarioIDForBaseline() == check.hasScenarioIDForBaseline();
            if(hasGeospatialID())
            {
                returnMe = returnMe && getGeospatialID().equals(check.getGeospatialID());
            }
            if(hasVariableID())
            {
                returnMe = returnMe && getVariableID().equals(check.getVariableID());
            }
            if(hasScenarioID())
            {
                returnMe = returnMe && getScenarioID().equals(check.getScenarioID());
            }
            if(hasScenarioIDForBaseline())
            {
                returnMe = returnMe && getScenarioIDForBaseline().equals(check.getScenarioIDForBaseline());
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

    private class LocationImpl implements Location
    {
        private final Long vectorIdentifier;
        private final String locationName;
        private final Float longitude;
        private final Float latitude;
        private final String gageId;

        private LocationImpl(final Long vectorIdentifier,
                             final String locationName,
                             final Float longitude,
                             final Float latitude,
                             final String gageId)
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
            if (this.hasLocationName())
            {
                return this.locationName;
            }

            if (this.hasGageId())
            {
                return this.gageId;
            }

            if (this.hasVectorIdentifier())
            {
                return this.vectorIdentifier.toString();
            }

            if (this.hasCoordinates())
            {
                return this.getLongitude() + " " + this.getLatitude();
            }

            return "Unknown";
        }

        @Override
        public boolean equals( Object obj )
        {
            if (obj instanceof Location)
            {
                Location other = (Location)obj;

                boolean locationsEqual = true;
                boolean vectorIDsEqual = true;
                boolean gageIDsEqual = true;
                boolean coordinatesEqual = true;

                if (this.hasLocationName() && other.hasLocationName())
                {
                    locationsEqual = this.getLocationName().equalsIgnoreCase( other.getLocationName() );
                }
                else if (this.hasLocationName() || other.hasLocationName())
                {
                    return false;
                }

                if (this.hasVectorIdentifier() && other.hasVectorIdentifier())
                {
                    vectorIDsEqual = this.getVectorIdentifier().equals( other.getVectorIdentifier() );
                }
                else if (this.hasVectorIdentifier() || other.hasVectorIdentifier())
                {
                    return false;
                }

                if (this.hasGageId() && other.hasGageId())
                {
                    gageIDsEqual = this.getGageId().equalsIgnoreCase( other.getGageId() );
                }
                else if (this.hasGageId() || other.hasGageId())
                {
                    return false;
                }

                if (this.hasCoordinates() && other.hasCoordinates())
                {
                    coordinatesEqual = this.getLatitude().equals( other.getLatitude() ) &&
                                       this.getLongitude().equals(other.getLongitude());
                }
                else if (this.hasCoordinates() || other.hasCoordinates())
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

    private DefaultMetadataFactory()
    {
    }
    
}
