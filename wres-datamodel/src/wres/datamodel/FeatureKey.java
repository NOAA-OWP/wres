package wres.datamodel;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import wres.statistics.generated.Geometry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a geographic feature. Db contents matches what is here.
 * 
 * TODO: This class should probably compose a canonical {@link Geometry} as it is 1:1 with the canonical representation.
 * See #73842-38 - #73842-40.
 */

public class FeatureKey implements Comparable<FeatureKey>
{
    private final String name;
    private final String description;
    private final Integer srid;
    private final String wkt;

    public FeatureKey( String name,
                       String description,
                       Integer srid,
                       String wkt )
    {
        this.name = name;
        this.description = description;
        this.srid = srid;
        this.wkt = wkt;
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

    public String getName()
    {
        return this.name;
    }

    public String getDescription()
    {
        return this.description;
    }

    public Integer getSrid()
    {
        return this.srid;
    }

    public String getWkt()
    {
        return this.wkt;
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
        FeatureKey that = (FeatureKey) o;
        return name.equals( that.name ) &&
               Objects.equals( description, that.description )
               &&
               Objects.equals( srid, that.srid )
               &&
               Objects.equals( wkt, that.wkt );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, description, srid, wkt );
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
                                                                            .append( "name", name )
                                                                            .append( "description", description )
                                                                            .append( "srid", srid )
                                                                            .append( "wkt", wkt )
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

}
