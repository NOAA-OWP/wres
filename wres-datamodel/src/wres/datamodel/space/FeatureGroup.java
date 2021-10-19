package wres.datamodel.space;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of {@link FeatureTuple}, which may be named.
 * 
 * @author James Brown
 */

public class FeatureGroup implements Comparable<FeatureGroup>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroup.class );

    /** An optional name for the group. */
    private final String name;

    /** The features. */
    private final SortedSet<FeatureTuple> features;

    /**
     * @param features the grouped features, not null, not empty
     * @return an instance
     * @throws NullPointerException if the features are null
     * @throws IllegalArgumentException if the features are empty
     */
    public static FeatureGroup of( Set<FeatureTuple> features )
    {
        return new FeatureGroup( null, features );
    }

    /**
     * @param name the group name, may be null
     * @param features the grouped features, not null, not empty
     * @return an instance
     * @throws NullPointerException if the features are null
     * @throws IllegalArgumentException if the features are empty
     */
    public static FeatureGroup of( String name, Set<FeatureTuple> features )
    {
        return new FeatureGroup( name, features );
    }

    /**
     * @param feature the feature, not null
     * @return an instance
     * @throws NullPointerException if the features are null
     */
    public static FeatureGroup of( FeatureTuple feature )
    {
        return FeatureGroup.of( null, feature );
    }

    /**
     * @param name the group name, may be null
     * @param feature the feature, not null, not empty
     * @return an instance
     * @throws NullPointerException if the feature is null
     * @throws IllegalArgumentException if the features are empty
     */
    public static FeatureGroup of( String name, FeatureTuple feature )
    {
        Objects.requireNonNull( feature );

        return new FeatureGroup( name, Collections.singleton( feature ) );
    }

    /**
     * @param features the features
     * @return one unnamed, singleton feature group for each feature in the input
     * @throws NullPointerException if the features are null
     */
    public static Set<FeatureGroup> ofSingletons( Set<FeatureTuple> features )
    {
        Objects.requireNonNull( features );

        return features.stream()
                       .map( FeatureGroup::of )
                       .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * @return the name
     */

    public String getName()
    {
        return this.name;
    }

    /**
     * @return the features
     */

    public Set<FeatureTuple> getFeatures()
    {
        return this.features; //Immutable on construction
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( ! ( o instanceof FeatureGroup ) )
        {
            return false;
        }

        FeatureGroup in = (FeatureGroup) o;

        return Objects.equals( in.getName(), this.getName() )
               && Objects.equals( in.getFeatures(), this.getFeatures() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getName(), this.getFeatures() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "name", this.getName() )
                                                                            .append( "features", this.getFeatures() )
                                                                            .toString();
    }

    @Override
    public int compareTo( FeatureGroup o )
    {
        Objects.requireNonNull( o );

        // Name
        int returnMe = Objects.compare( this.name, o.name, Comparator.nullsFirst( String::compareTo ) );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Size of group
        returnMe = Integer.compare( this.features.size(), o.features.size() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Group members, which belong to a sorted set
        Iterator<FeatureTuple> iterator = o.features.iterator();
        for ( FeatureTuple next : this.features )
        {
            returnMe = next.compareTo( iterator.next() );

            if ( returnMe != 0 )
            {
                return returnMe;
            }
        }

        return 0;
    }

    /**
     * Hidden constructor.
     * @param name the group name, may be null
     * @param features the grouped features, not null, not empty
     * @throws NullPointerException if the features are null
     * @throws IllegalArgumentException if the features are empty
     */

    private FeatureGroup( String name, Set<FeatureTuple> features )
    {
        Objects.requireNonNull( features );

        if ( features.isEmpty() )
        {
            throw new IllegalArgumentException( "A feature group requires one or more features." );
        }

        this.name = name;
        this.features = Collections.unmodifiableSortedSet( new TreeSet<>( features ) );

        LOGGER.debug( "Created a feature group with name {} and features {}.", this.getName(), this.getFeatures() );
    }

}
