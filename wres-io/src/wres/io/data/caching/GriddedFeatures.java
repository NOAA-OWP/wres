package wres.io.data.caching;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.LatLonPoint;
import wres.config.generated.Circle;
import wres.config.generated.Polygon;
import wres.config.generated.Polygon.Point;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;
import wres.statistics.generated.Geometry;
import wres.util.NetCDF;

/**
 * Finds and caches gridded features associated with an evaluation.
 * 
 * @author James Brown
 */
@Immutable
public class GriddedFeatures implements Supplier<Set<Feature>>
{
    /** The features.*/
    private final Set<Feature> features;

    /** The logger.*/
    private static final Logger LOGGER = LoggerFactory.getLogger( GriddedFeatures.class );

    /** Re-used string.*/
    private static final String POINT = "POINT(";

    @Override
    public Set<Feature> get()
    {
        return this.features; // Immutable on construction
    }

    /**
     * Hidden constructor.
     * @param builder the builder
     */

    private GriddedFeatures( Builder builder )
    {
        Set<Feature> innerFeatures = new HashSet<>( builder.features );
        this.features = Collections.unmodifiableSet( innerFeatures );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Created an instance of GriddedFeatures with {} features.", this.features.size() );
        }
    }

    /**
     * Builder.
     */
    public static class Builder
    {
        /**
         * The features.
         */

        private final Set<Feature> features = ConcurrentHashMap.newKeySet();

        /**
         * The filters.
         */

        private final List<UnnamedFeature> filters;

        /**
         * Cache of metadatas to check, guarded by the {@link #metadataGuard}.
         */
        @GuardedBy( "metadataGuard" )
        private final Set<GridMetadata> metadata = new HashSet<>();

        /**
         * Guards the {@link #metadata}.
         */

        private final Object metadataGuard = new Object();

        /**
         * @return an instance of {@link GriddedFeatures}
         */
        public GriddedFeatures build()
        {
            return new GriddedFeatures( this );
        }

        /**
         * @param source the file containing a grid
         * @throws IOException if the source could not be read for any reason
         * @return the builder
         */

        public Builder addFeatures( NetcdfFile source ) throws IOException
        {
            GridMetadata test = new GridMetadata( source );

            synchronized ( this.metadataGuard )
            {
                if ( this.metadata.contains( test ) )
                {
                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "Skipping the reading and filtering of coordinates for gridded dataset {} "
                                      + "because an equivalent grid was already read.",
                                      source.getLocation() );
                    }

                    return this;
                }
                else
                {
                    this.metadata.add( test );
                }
            }

            Set<Feature> innerFeatures = GriddedFeatures.readFeaturesAndFilterThem( source, this.filters );
            this.features.addAll( innerFeatures );

            return this;
        }

        /**
         * Creates an instance.
         * @param filters the filters, not null and not empty
         * @throws NullPointerException if the filters is null
         * @throws IllegalArgumentException if there are no filters
         */

        public Builder( List<UnnamedFeature> filters )
        {
            Objects.requireNonNull( filters );

            if ( filters.isEmpty() )
            {
                throw new IllegalArgumentException( "Cannot determine gridded features without a grid selection. An "
                                                    + "unbounded selection is not currently supported. Please declare "
                                                    + "a grid selection and try again." );
            }

            this.filters = List.copyOf( filters );
        }
    }

    /**
     * @param source the grid
     * @param filters the filters
     * @return the features
     * @throws IOException if the source could not be read for any reason
     * @throws NullPointerException if any input is null
     */

    private static Set<Feature> readFeaturesAndFilterThem( NetcdfFile source, List<UnnamedFeature> filters )
            throws IOException
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( filters );

        Set<Feature> innerFeatures = new HashSet<>();

        try ( NetcdfDataset ncd = NetcdfDatasets.openDataset( source.getLocation() );
              GridDataset grid = new GridDataset( ncd ) )
        {
            GridCoordSystem coordinateSystem = grid.getGrids().get( 0 ).getCoordinateSystem();

            Variable xCoordinates = NetCDF.getVariable( source, "x" );
            Variable yCoordinates = NetCDF.getVariable( source, "y" );

            for ( int xIndex = 0; xIndex < xCoordinates.getSize(); ++xIndex )
            {
                for ( int yIndex = 0; yIndex < yCoordinates.getSize(); ++yIndex )
                {
                    LatLonPoint point = coordinateSystem.getLatLon( xIndex, yIndex );

                    // Within the filter boundaries?
                    if ( GriddedFeatures.isContained( point, filters ) )
                    {
                        Feature tuple = GriddedFeatures.getFeatureFromCoordinate( point );
                        innerFeatures.add( tuple );
                    }

                }
            }
        }

        LOGGER.info( "Finished reading and filtering coordinates for gridded dataset {}. Discovered {} features to "
                     + "use.",
                     source.getLocation(),
                     innerFeatures.size() );

        return Collections.unmodifiableSet( innerFeatures );
    }

    /**
     * @param point the point
     * @return the feature
     * @throws IllegalArgumentException if the longitude or latitude are invalid
     */

    private static Feature getFeatureFromCoordinate( LatLonPoint point )
    {
        double x = point.getLongitude();
        double y = point.getLatitude();

        StringJoiner wktBuilder =
                new StringJoiner( " " );
        wktBuilder.add( POINT );
        wktBuilder.add( Double.toString( x ) );
        wktBuilder.add( Double.toString( y ) );
        wktBuilder.add( ")" );
        String wkt = wktBuilder.toString();

        GriddedFeatures.validateLonLat( x, y );

        Geometry geometry = MessageFactory.getGeometry( GriddedFeatures.getGriddedNameFromLonLat( x, y ),
                                                        GriddedFeatures.getGriddedDescriptionFromLonLat( x, y ),
                                                        4326,
                                                        wkt );

        return Feature.of( geometry );
    }

    /**
     * TODO: remove casting to float and update any test benchmarks.
     * @param x the longitude value
     * @param y the latitude value
     * @return the name with E or W and N or S.
     */
    private static String getGriddedDescriptionFromLonLat( double x, double y )
    {
        String name;

        if ( x < 0 )
        {
            name = Math.abs( (float) x ) + "W_";
        }
        else
        {
            name = (float) x + "E_";
        }

        if ( y < 0 )
        {
            name += Math.abs( (float) y ) + "S";
        }
        else
        {
            name += (float) y + "N";
        }

        return name;
    }

    /**
     * TODO: remove casting to float and update any test benchmarks.
     * @param x the longitude value
     * @param y the latitude value
     * @return the name
     */
    private static String getGriddedNameFromLonLat( double x, double y )
    {
        return (float) x + " " + (float) y;
    }

    /**
     * @throws IllegalArgumentException when longitude or latitude out of range
     */
    private static void validateLonLat( double x, double y )
    {
        if ( x < -180.0 || x > 180.0 )
        {
            throw new IllegalArgumentException( "Expected longitude x between -180.0 and 180.0, got "
                                                + x );
        }

        if ( y < -90.0 || y > 90.0 )
        {
            throw new IllegalArgumentException( "Expected latitude y between -90.0 and 90.0, got "
                                                + y );
        }
    }

    /**
     * @param point the point
     * @param filters the filters
     * @return true if the point is contained within the requested selection, otherwise false
     */
    private static boolean isContained( LatLonPoint point, List<UnnamedFeature> filters )
    {
        boolean contained = !filters.isEmpty();
        for ( UnnamedFeature feature : filters )
        {
            if ( Objects.nonNull( feature.getPolygon() ) )
            {
                contained = contained && GriddedFeatures.isContained( point, feature.getPolygon() );
            }

            if ( Objects.nonNull( feature.getCircle() ) )
            {
                contained = contained && GriddedFeatures.isContained( point, feature.getCircle() );
            }

            if ( Objects.nonNull( feature.getCoordinate() ) )
            {
                // #90061-42
                throw new UnsupportedOperationException( "Coordinate selection of gridded features is not currently "
                                                         + "supported. Please use a polygon selection instead and try "
                                                         + "again." );
            }
        }

        return contained;
    }

    /**
     * TODO: support concave polygons by making a library call
     * @param point the point
     * @param convexPolygon the convex polygon filter
     * @return true if the point is contained within the convex polygon, otherwise false
     */
    private static boolean isContained( LatLonPoint point, Polygon convexPolygon )
    {
        double x = point.getLongitude();
        double y = point.getLatitude();

        List<Point> points = convexPolygon.getPoint();

        int i;
        int j;
        boolean result = false;
        for ( i = 0, j = points.size() - 1; i < points.size(); j = i++ )
        {
            if ( ( points.get( i ).getLatitude() > y ) != ( points.get( j ).getLatitude() > y ) &&
                 ( x < ( points.get( j ).getLongitude() - points.get( i ).getLongitude() )
                       * ( y - points.get( i ).getLatitude() )
                       / ( points.get( j ).getLatitude() - points.get( i ).getLatitude() )
                       + points.get( i ).getLongitude() ) )
            {
                result = !result;
            }
        }

        return result;
    }

    /**
     * @param point the point
     * @param circle the circle filter
     * @return true if the point is contained within the circle, otherwise false
     */
    private static boolean isContained( LatLonPoint point, Circle circle )
    {
        double x = point.getLongitude();
        double y = point.getLatitude();

        double x2 = circle.getLongitude();
        double y2 = circle.getLatitude();
        double diameter = circle.getDiameter();

        return ( Math.pow( x - x2, 2 ) + Math.pow( y - y2, 2 ) ) < Math.pow( diameter / 2.0, 2 );
    }

    /**
     * Small class that stores the grid metadata, in order to avoid reading grids with the same metadata multiple times.
     */

    private static class GridMetadata
    {
        private final String srText;
        private final String proj4;
        private final String projectionMapping;
        private final String xUnit;
        private final String yUnit;
        private final String xType;
        private final String yType;

        private final Number xResolution;
        private final Number yResolution;

        private final long xSize;
        private final long ySize;

        /** Cache of hash. Volatile to avoid recomputing across cpus. */
        private volatile int hash;

        /**
         * Construct.
         */

        private GridMetadata( NetcdfFile file )
        {
            Variable xCoordinates = NetCDF.getVariable( file, "x" );
            Variable yCoordinates = NetCDF.getVariable( file, "y" );
            Variable coordinateSystem = NetCDF.getVariable( file, "ProjectionCoordinateSystem" );

            if ( Objects.isNull( coordinateSystem ) )
            {
                coordinateSystem = NetCDF.getVariable( file, "crs" );

                if ( Objects.isNull( coordinateSystem ) )
                {
                    throw new IllegalStateException( "While reading a netcdf blob, failed to discover the coordinate "
                                                     + "system variable as either 'ProjectionCoordinateSystem' or "
                                                     + "'crs'. The blob metadata is: "
                                                     + file );
                }
            }

            this.srText = coordinateSystem.findAttributeString( "esri_pe_string", "" );
            this.proj4 = coordinateSystem.findAttributeString( "proj4", "" );
            this.projectionMapping =
                    coordinateSystem.findAttributeString( "grid_mapping_name", "lambert_conformal_conic" );

            Attribute xRes = xCoordinates.findAttribute( "resolution" );

            if ( Objects.isNull( xRes ) )
            {
                throw new IllegalStateException( "While reading a netcdf blob, failed to discover a required attribute "
                                                 + "for the X coordinate dimension, namely: 'resolution'. The blob "
                                                 + "metadata is: "
                                                 + file );
            }

            Attribute yRes = yCoordinates.findAttribute( "resolution" );

            if ( Objects.isNull( yRes ) )
            {
                throw new IllegalStateException( "While reading a netcdf file, failed to discover a required attribute "
                                                 + "for the Y coordinate dimension, namely: 'resolution'. The blob "
                                                 + "metadata is: "
                                                 + file );
            }

            this.xResolution = xRes.getNumericValue();
            this.yResolution = yRes.getNumericValue();
            this.xSize = xCoordinates.getSize();
            this.ySize = yCoordinates.getSize();
            this.xUnit = xCoordinates.findAttributeString( "units", "" );
            this.yUnit = yCoordinates.findAttributeString( "units", "" );
            this.xType = xCoordinates.findAttributeString( "_CoordinateAxisType", "GeoX" );
            this.yType = yCoordinates.findAttributeString( "_CoordinateAxisType", "GeoY" );
        }

        @Override
        public int hashCode()
        {
            if ( this.hash == 0 )
            {
                this.hash = Objects.hash( this.srText,
                                          this.proj4,
                                          this.projectionMapping,
                                          this.xResolution,
                                          this.yResolution,
                                          this.xSize,
                                          this.ySize,
                                          this.xUnit,
                                          this.yUnit,
                                          this.xType,
                                          this.yType );
            }

            return this.hash;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == this )
            {
                return true;
            }

            if ( ! ( obj instanceof GridMetadata in ) )
            {
                return false;
            }

            return Objects.equals( this.srText, in.srText )
                   && Objects.equals( this.proj4, in.proj4 )
                   && Objects.equals( this.projectionMapping, in.projectionMapping )
                   && Objects.equals( this.xResolution, in.xResolution )
                   && Objects.equals( this.yResolution, in.yResolution )
                   && Objects.equals( this.xUnit, in.xUnit )
                   && Objects.equals( this.yUnit, in.yUnit )
                   && Objects.equals( this.xType, in.xType )
                   && Objects.equals( this.yType, in.yType )
                   && this.xSize == in.xSize
                   && this.ySize == in.ySize;
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "srText", this.srText )
                                                                                .append( "proj4", this.proj4 )
                                                                                .append( "projectionMapping",
                                                                                         this.projectionMapping )
                                                                                .append( "xResolution",
                                                                                         this.xResolution )
                                                                                .append( "yResolution",
                                                                                         this.yResolution )
                                                                                .append( "xUnit", this.xUnit )
                                                                                .append( "yUnit", this.yUnit )
                                                                                .append( "xType", this.xType )
                                                                                .append( "yType", this.yType )
                                                                                .append( "xSize", this.xSize )
                                                                                .append( "ySize", this.ySize )
                                                                                .toString();
        }
    }


}
