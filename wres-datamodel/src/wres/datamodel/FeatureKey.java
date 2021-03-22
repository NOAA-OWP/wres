package wres.datamodel;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import wres.statistics.generated.Geometry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a geographic feature. Db contents matches what is here. Composes a canonical {@link Geometry}.
 */

public class FeatureKey implements Comparable<FeatureKey>
{
    private final Geometry geometry;

    public FeatureKey( String name,
                       String description,
                       Integer srid,
                       String wkt )
    {
        Geometry.Builder builder = Geometry.newBuilder();

        if ( Objects.nonNull( name ) )
        {
            builder.setName( name );
        }

        if ( Objects.nonNull( description ) )
        {
            builder.setDescription( description );
        }

        if ( Objects.nonNull( srid ) )
        {
            builder.setSrid( srid );
        }

        if ( Objects.nonNull( wkt ) )
        {
            builder.setWkt( wkt );
        }

        this.geometry = builder.build();
    }

    /**
     * Replaces wres.datamodel.FeatureKey.of
     * @param name The lid or gage id.
     * @return feature.
     */

    public static FeatureKey of( String name )
    {
        return new FeatureKey( name, null, null, null );
    }

    /**
     * Returns an instance from a {@link Geometry}.
     * 
     * @param geometry the geometry
     * @return an instance from a geometry
     * @throws NullPointerException if the geometry is null
     */

    public static FeatureKey of( Geometry geometry )
    {
        return new FeatureKey( geometry );
    }

    public String getName()
    {
        return this.geometry.getName();
    }

    public String getDescription()
    {
        return this.geometry.getDescription();
    }

    public Integer getSrid()
    {
        return this.geometry.getSrid();
    }

    public String getWkt()
    {
        return this.geometry.getWkt();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Geometry in = ( (FeatureKey) o ).geometry;

        return this.geometry.equals( in );
    }

    @Override
    public int hashCode()
    {
        return this.geometry.hashCode();
    }

    @Override
    public int compareTo( FeatureKey o )
    {
        if ( this.equals( o ) )
        {
            return 0;
        }

        int nameComparison = this.getName()
                                 .compareTo( o.getName() );

        if ( nameComparison != 0 )
        {
            return nameComparison;
        }

        if ( Objects.nonNull( this.getDescription() ) )
        {
            if ( Objects.nonNull( o.getDescription() ) )
            {
                int descriptionComparison = this.getDescription()
                                                .compareTo( o.getDescription() );
                if ( descriptionComparison != 0 )
                {
                    return descriptionComparison;
                }
            }
            else
            {
                return 1;
            }
        }
        else if ( Objects.nonNull( o.getDescription() ) )
        {
            return -1;
        }

        if ( Objects.nonNull( this.getSrid() ) )
        {
            if ( Objects.nonNull( o.getSrid() ) )
            {
                int sridComparison = this.getSrid()
                                         .compareTo( o.getSrid() );
                if ( sridComparison != 0 )
                {
                    return sridComparison;
                }
            }
            else
            {
                return 1;
            }
        }
        else if ( Objects.nonNull( o.getSrid() ) )
        {
            return -1;
        }

        if ( Objects.nonNull( this.getWkt() ) )
        {
            if ( Objects.nonNull( o.getWkt() ) )
            {
                int wktComparison = this.getWkt()
                                        .compareTo( o.getWkt() );
                if ( wktComparison != 0 )
                {
                    return wktComparison;
                }
            }
            else
            {
                return 1;
            }
        }
        else if ( Objects.nonNull( o.getWkt() ) )
        {
            return -1;
        }

        throw new IllegalStateException( "Could not find the difference between FeatureKey "
                                         + this
                                         + " and FeatureKey "
                                         + o );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "name", this.getName() )
                                                                            .append( "description",
                                                                                     this.getDescription() )
                                                                            .append( "srid", this.getSrid() )
                                                                            .append( "wkt", this.getWkt() )
                                                                            .toString();
    }


    /**
     * Geometric or geographic point with double x and double y.
     */

    public static class GeoPoint
    {
        private final double x;
        private final double y;

        public GeoPoint( double x, double y )
        {
            this.x = x;
            this.y = y;
        }

        public double getX()
        {
            return this.x;
        }

        public double getY()
        {
            return this.y;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            GeoPoint geoPoint = (GeoPoint) o;
            return Double.compare( geoPoint.getX(), getX() ) == 0 &&
                   Double.compare( geoPoint.getY(), getY() ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( getX(), getY() );
        }

        @Override
        public String toString()
        {
            return new ToStringBuilder( this )
                                              .append( "x", x )
                                              .append( "y", y )
                                              .toString();
        }
    }


    /**
     * Return x,y from a wkt with a POINT in it.
     * As of 2020-06-30 only used for gridded evaluation.
     * @param wkt A well-known text geometry with POINT
     * @return GeoPoint with x,y doubles parsed
     * @throws IllegalArgumentException When no "POINT" found or parsing fails
     */

    public static GeoPoint getLonLatFromPointWkt( String wkt )
    {
        String wktUpperCase = wkt.strip()
                                 .toUpperCase();

        // Validate it's a point
        if ( !wktUpperCase.startsWith( "POINT" ) ||
             !wktUpperCase.contains( "(" )
             ||
             !wktUpperCase.contains( ")" ) )
        {
            throw new IllegalArgumentException( "Only able to support POINT for gridded selection" );
        }

        double x;
        double y;
        String[] wktParts = wktUpperCase.split( "[ )(]", 30 );
        List<String> parts = Arrays.stream( wktParts )
                                   .filter( s -> !s.isBlank() )
                                   .collect( Collectors.toList() );

        if ( parts.size() != 3 )
        {
            throw new IllegalArgumentException( "Only found these elements from wkt, expected three: "
                                                + parts );
        }

        try
        {
            x = Double.parseDouble( parts.get( 1 ) );
            y = Double.parseDouble( parts.get( 2 ) );
        }
        catch ( NumberFormatException nfe )
        {
            throw new IllegalArgumentException( "Unable to parse coordinates from this wkt: "
                                                + wkt );
        }

        return new GeoPoint( x, y );
    }

    /**
     * Return x,y from a wkt with a POINT in it.
     * A less strict version of the above method, returns null if anything goes
     * wrong. The above strict version is needed for gridded evaluation whereas
     * this one will return null when it cannot get a GeoPoitn out of the wkt.
     * @param wkt A well-known text geometry that may have POINT
     * @return GeoPoint with x,y doubles parsed, null when anything goes wrong
     */

    public static GeoPoint getLonLatOrNullFromWkt( String wkt )
    {
        String wktUpperCase = wkt.strip()
                                 .toUpperCase();

        // Validate it's a point
        if ( !wktUpperCase.startsWith( "POINT" ) ||
             !wktUpperCase.contains( "(" )
             ||
             !wktUpperCase.contains( ")" ) )
        {
            // Not a point, cannot get a single point from a non-point.
            return null;
        }

        double x;
        double y;
        String[] wktParts = wktUpperCase.split( "[ )(]", 30 );
        List<String> parts = Arrays.stream( wktParts )
                                   .filter( s -> !s.isBlank() )
                                   .collect( Collectors.toList() );

        if ( parts.size() < 3 || parts.size() > 4 )
        {
            // Too few or too many eleements parsed, cannot proceed.
            return null;
        }

        try
        {
            x = Double.parseDouble( parts.get( 1 ) );
            y = Double.parseDouble( parts.get( 2 ) );
        }
        catch ( NumberFormatException nfe )
        {
            // Could not parse a double, cannot proceed.
            return null;
        }

        return new GeoPoint( x, y );
    }

    /**
     * Hidden constructor.
     * @param geometry a geometry
     * @throws NullPointerException if the geometry is null
     */

    private FeatureKey( Geometry geometry )
    {
        Objects.requireNonNull( geometry );

        this.geometry = geometry;
    }
}
