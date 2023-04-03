package wres.datamodel.space;

import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.messages.MessageUtilities;
import wres.statistics.generated.GeometryTuple;

/**
 * Represents a tuple of geographic features. Wraps a canonical {@link GeometryTuple} and adds behavior.
 */

public class FeatureTuple implements Comparable<FeatureTuple>
{
    /** The canonical tuple. */
    private final GeometryTuple geometryTuple;

    /**
     * @param geometryTuple the geometry tuple
     * @return an instance
     */
    public static FeatureTuple of( GeometryTuple geometryTuple )
    {
        return new FeatureTuple( geometryTuple );
    }

    /**
     * @return the left feature
     */
    public Feature getLeft()
    {
        return Feature.of( this.geometryTuple.getLeft() );
    }

    /**
     * @return the right feature
     */
    public Feature getRight()
    {
        return Feature.of( this.geometryTuple.getRight() );
    }

    /**
     * @return the baseline feature
     */
    public Feature getBaseline()
    {
        Feature returnMe = null;

        if ( this.geometryTuple.hasBaseline() )
        {
            returnMe = Feature.of( this.geometryTuple.getBaseline() );
        }

        return returnMe;
    }

    /**
     * @return the geometry tuple
     */
    public GeometryTuple getGeometryTuple()
    {
        return this.geometryTuple;
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
        return this.getGeometryTuple().equals( that.getGeometryTuple() );
    }

    @Override
    public int hashCode()
    {
        return this.geometryTuple.hashCode();
    }

    @Override
    public int compareTo( FeatureTuple o )
    {
        return MessageUtilities.compare( this.getGeometryTuple(), o.getGeometryTuple() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "left", this.getLeft() )
                .append( "right", this.getRight() )
                .append( "baseline", this.getBaseline() )
                .toString();
    }

    /**
     * @return a short string representation of the feature tuple, comprising the names only.
     */

    public String toStringShort()
    {
        String separator = "-";

        // Other sides likely to use similar naming convention, such as coordinate pairs, which may be negative, '-'
        if ( Objects.nonNull( this.getLeft().getName() ) && this.getLeft().getName().contains( "-" ) )
        {
            separator = ", ";
        }

        StringJoiner joiner = new StringJoiner( separator );

        joiner.add( this.getLeftName() ).add( this.getRightName() );

        if ( Objects.nonNull( this.getBaseline() ) )
        {
            joiner.add( this.getBaselineName() );
        }

        return joiner.toString();
    }

    /**
     * Get the name of the left feature. Convenience method.
     * @return The name of the left feature.
     */

    public String getLeftName()
    {
        return this.getLeft()
                   .getName();
    }

    /**
     * Get the name of the right feature. Convenience method.
     * @return The name of the right feature.
     */

    public String getRightName()
    {
        return this.getRight()
                   .getName();
    }

    /**
     * Get the name of the baseline feature. Convenience method.
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
     * Get the name of the feature based on the left/right/baseline given. Convenience method.
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

    /**
     * Hidden constructor.
     * @param geometryTuple the geometry tuple
     * @throws NullPointerException if the input is null
     */

    private FeatureTuple( GeometryTuple geometryTuple )
    {
        Objects.requireNonNull( geometryTuple );

        this.geometryTuple = geometryTuple;
    }

}
