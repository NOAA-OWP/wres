package wres.datamodel.pools;

import java.util.Comparator;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A composition of up to two {@link PoolMetadata}. A {@link PoolRequest} drives the creation of a {@link Pool} and 
 * its contained {@link PoolMetadata} describe that pool once created. There is one {@link PoolMetadata} for each 
 * combination of left/right and left/baseline datasets within an evaluation because each pair of datasets may represent
 * its own pools for metric calculation, while the composition may be used for comparative metrics, such as skill.
 * 
 * TODO: candidate to replace with a Java Record in JDK 17.
 * 
 * @author James Brown
 */

public class PoolRequest implements Comparable<PoolRequest>
{
    /** The metadata for the left/right data within the pool. */
    private final PoolMetadata leftRight;

    /** The metadata for the left/baseline data within the pool. */
    private final PoolMetadata leftBaseline;

    /**
     * @param leftRight the left/right pool metadata, not null
     * @return an instance
     * @throws NullPointerException if the left/right metadata is null
     */

    public static PoolRequest of( PoolMetadata leftRight )
    {
        return new PoolRequest( leftRight, null );
    }
    
    /**
     * @param leftRight the left/right pool metadata, not null
     * @param leftBaseline the left/baseline pool metadata, possibly null
     * @return an instance
     * @throws NullPointerException if the left/right metadata is null
     */

    public static PoolRequest of( PoolMetadata leftRight, PoolMetadata leftBaseline )
    {
        return new PoolRequest( leftRight, leftBaseline );
    }

    /**
     * @return true if there is left/baseline metadata, otherwise false
     */

    public boolean hasBaseline()
    {
        return Objects.nonNull( this.getMetadataForBaseline() );
    }

    /**
     * @return the left/right metadata
     */

    public PoolMetadata getMetadata()
    {
        return this.leftRight;
    }

    /**
     * @return the left/baseline metadata
     */

    public PoolMetadata getMetadataForBaseline()
    {
        return this.leftBaseline;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }
        if ( ! ( o instanceof PoolRequest ) )
        {
            return false;
        }

        PoolRequest in = (PoolRequest) o;

        return Objects.equals( in.getMetadata(), this.getMetadata() )
               && Objects.equals( in.getMetadataForBaseline(), this.getMetadataForBaseline() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getMetadata(), this.getMetadataForBaseline() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "metadata", this.getMetadata() )
                                                                            .append( "baselineMetadata",
                                                                                     this.getMetadataForBaseline() )
                                                                            .toString();
    }
    
    @Override
    public int compareTo( PoolRequest o )
    {
        Objects.requireNonNull( o );

        int compare = this.getMetadata()
                          .compareTo( o.getMetadata() );

        if ( compare != 0 )
        {
            return compare;
        }

        return Objects.compare( this.getMetadataForBaseline(),
                                o.getMetadataForBaseline(),
                                Comparator.nullsFirst( Comparator.naturalOrder() ) );
    }

    /**
     * Hidden constructor.
     * @param leftRight the left/right metadata
     * @param leftBaseline the left/baseline metadata
     * @throws NullPointerException if the left/right metadata is null
     */

    private PoolRequest( PoolMetadata leftRight, PoolMetadata leftBaseline )
    {
        Objects.requireNonNull( leftRight );

        this.leftRight = leftRight;
        this.leftBaseline = leftBaseline;
    }
}
