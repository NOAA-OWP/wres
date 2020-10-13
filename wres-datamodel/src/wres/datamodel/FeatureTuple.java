package wres.datamodel;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.statistics.generated.GeometryTuple;

/**
 * Vector features correlated either as declared or by a service call. Contains
 * at least a left and right, optionally a baseline. Optionally (eventually) a
 * climatology.
 * 
 * TODO: This class should probably compose a canonical {@link GeometryTuple} as it is 1:1 with the canonical 
 * representation. See #73842-38 - #73842-40.
 */

public class FeatureTuple implements Comparable<FeatureTuple>
{
    private final FeatureKey left;
    private final FeatureKey right;
    private final FeatureKey baseline;

    public FeatureTuple( FeatureKey left, FeatureKey right, FeatureKey baseline )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );
        this.left = left;
        this.right = right;
        this.baseline = baseline;
    }

    public FeatureKey getLeft()
    {
        return this.left;
    }

    public FeatureKey getRight()
    {
        return this.right;
    }

    public FeatureKey getBaseline()
    {
        return this.baseline;
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
        FeatureTuple that = ( FeatureTuple ) o;
        return left.equals( that.left ) &&
               right.equals( that.right ) &&
               Objects.equals( baseline, that.baseline );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( left, right, baseline );
    }


    @Override
    public int compareTo( FeatureTuple o )
    {
        if ( this.equals( o ) )
        {
            return 0;
        }

        int leftComparison = this.getLeft()
                                 .compareTo( o.getLeft() );

        if ( leftComparison != 0 )
        {
            return leftComparison;
        }

        int rightComparison = this.getRight()
                                  .compareTo( o.getRight() );

        if ( rightComparison != 0)
        {
            return rightComparison;
        }

        if ( Objects.nonNull( this.getBaseline() ) )
        {
            if ( Objects.nonNull( o.getBaseline() ) )
            {
                return this.getBaseline()
                           .compareTo( o.getBaseline() );
            }

            return 1;
        }

        throw new IllegalStateException( "Could not find the difference between FeatureTuple "
                                         + this + " and FeatureTuple " + o );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "left", left )
                .append( "right", right )
                .append( "baseline", baseline )
                .toString();
    }

    /**
     * @return a short string representation of the feature tuple, comprising the names only.
     */
    
    public String toStringShort()
    {
        StringJoiner joiner = new StringJoiner( "-" );
        
        joiner.add( this.getLeftName() ).add( this.getRightName() );
        
        if( Objects.nonNull( this.getBaseline() ) )
        {
            joiner.add( this.getBaselineName() );
        }
        
        return joiner.toString();
    }
    
    /**
     * Get the name of the left feature.
     *
     * Convenience method.
     * @return The name of the left feature.
     */

    public String getLeftName()
    {
        return this.getLeft()
                   .getName();
    }

    /**
     * Get the name of the right feature.
     *
     * Convenience method.
     * @return The name of the right feature.
     */

    public String getRightName()
    {
        return this.getRight()
                   .getName();
    }


    /**
     * Get the name of the baseline feature.
     *
     * Convenience method.
     * @return The name of the baseline feature, null if no baseline.
     */

    public String getBaselineName()
    {
        if ( Objects.isNull( this.getBaseline() ) )
        {
            return null;
        }

        return this.getBaseline()
                   .getName();
    }


    /**
     * Get the name of the feature based on the left/right/baseline given.
     *
     * Convenience method.
     * @param leftOrRightOrBaseline Which name to get.
     * @return The name of the feature according to the l/r/b dataset.
     */

    public String getNameFor( LeftOrRightOrBaseline leftOrRightOrBaseline )
    {
        if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            return this.getLeftName();
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            return this.getRightName();
        }
        else if ( leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.BASELINE ) )
        {
            return this.getBaselineName();
        }
        else
        {
            throw new UnsupportedOperationException( "Cannot handle non-Left/Right/Baseline" );
        }
    }
}
