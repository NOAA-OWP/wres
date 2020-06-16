package wres.datamodel;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Vector features correlated either as declared or by a service call. Contains
 * at least a left and right, optionally a baseline. Optionally (eventually) a
 * climatology.
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

        return -1;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "left", left )
                .append( "right", right )
                .append( "baseline", baseline )
                .toString();
    }
}
