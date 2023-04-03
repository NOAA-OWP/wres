package wres.datamodel.space;

import java.util.Comparator;
import java.util.Objects;

import wres.statistics.generated.Geometry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A geographic feature. Wraps a canonical {@link Geometry} and adds behavior.
 *
 * @author James Brown
 * @author Jesse Bickel
 */

public class Feature implements Comparable<Feature>
{
    /** The canonical geometry wrapped by this instance. */
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
