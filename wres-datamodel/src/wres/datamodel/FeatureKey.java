package wres.datamodel;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a geographic feature. Db contents matches what is here.
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

    public FeatureKey of( String name )
    {
        return new FeatureKey ( name, null, null, null );
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
        FeatureKey that = ( FeatureKey ) o;
        return name.equals( that.name ) &&
               Objects.equals( description, that.description ) &&
               Objects.equals( srid, that.srid ) &&
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
                                         + this + " and FeatureKey " + o );
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
}
