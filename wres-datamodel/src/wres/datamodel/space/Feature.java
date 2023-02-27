package wres.datamodel.space;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import wres.statistics.generated.Geometry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a geographic feature. Composes a canonical {@link Geometry}.
 */

public class Feature implements Comparable<Feature>
{
    private final Geometry geometry;

    /**
     * Returns an instance from a {@link Geometry}.
     *
     * @param geometry the geometry
     * @return an instance from a geometry
     * @throws NullPointerException if the geometry is null
     */

    public static Feature of( Geometry geometry )
    {
        return new Feature( geometry );
    }

    /**
     * @return the feature name
     */
    public String getName()
    {
        return this.geometry.getName();
    }

    /**
     * @return the feature description
     */
    public String getDescription()
    {
        return this.geometry.getDescription();
    }

    /**
     * @return the spatial reference identifier
     */
    public Integer getSrid()
    {
        return this.geometry.getSrid();
    }

    /**
     * @return the wkt string
     */
    public String getWkt()
    {
        return this.geometry.getWkt();
    }

    /**
     * @return the geometry
     */
    public Geometry getGeometry()
    {
        return this.geometry;
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

        Geometry in = ( ( Feature ) o ).geometry;

        return this.geometry.equals( in );
    }

    @Override
    public int hashCode()
    {
        return this.geometry.hashCode();
    }

    @Override
    public int compareTo( Feature o )
    {
        int nameComparison = this.getName()
                                 .compareTo( o.getName() );

        if ( nameComparison != 0 )
        {
            return nameComparison;
        }

        int descriptionComparison = Comparator.nullsFirst( String::compareTo )
                                              .compare( this.getDescription(),
                                                        o.getDescription() );

        if ( descriptionComparison != 0 )
        {
            return descriptionComparison;
        }

        int sridComparison = Comparator.nullsFirst( Integer::compareTo )
                                       .compare( this.getSrid(),
                                                 o.getSrid() );

        if ( sridComparison != 0 )
        {
            return sridComparison;
        }

        return Comparator.nullsFirst( String::compareTo )
                         .compare( this.getWkt(),
                                   o.getWkt() );
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

    public record GeoPoint( double x, double y )
    {
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
            GeoPoint geoPoint = ( GeoPoint ) o;
            return Double.compare( geoPoint.x(), x() ) == 0 &&
                   Double.compare( geoPoint.y(), y() ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( x(), y() );
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
                                   .toList();

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
                                   .toList();

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

    private Feature( Geometry geometry )
    {
        Objects.requireNonNull( geometry );

        this.geometry = geometry;
    }
}
