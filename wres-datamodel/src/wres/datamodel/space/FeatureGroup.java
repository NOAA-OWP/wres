package wres.datamodel.space;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.datamodel.messages.MessageUtilities;
import wres.statistics.generated.GeometryGroup;

/**
 * A group of geographic feature tuples. Wraps a canonical {@link GeometryGroup} and adds behavior.
 * 
 * @author James Brown
 */

public class FeatureGroup implements Comparable<FeatureGroup>
{
    /** The maximum length of a feature group name: 32 characters for each name in a feature tuple, plus two separator 
     * characters. */
    public static final int MAXIMUM_NAME_LENGTH = 98;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroup.class );

    /** Cache of geometry groups, one per class loader. Unlimited number. */
    private static final Cache<GeometryGroup, FeatureGroup> FEATURE_GROUP_CACHE = Caffeine.newBuilder()
                                                                                          .build();

    /** The canonical group. */
    private final GeometryGroup geometryGroup;

    /** The wrapped features, cached for convenience. */
    private final SortedSet<FeatureTuple> features;

    /**
     * Creates an instance from a canonical instance.
     * 
     * @param geometryGroup the geometry group
     * @return an instance
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the group is empty
     */
    public static FeatureGroup of( GeometryGroup geometryGroup )
    {
        // Check the cache
        FeatureGroup cached = FEATURE_GROUP_CACHE.getIfPresent( geometryGroup );
        if ( Objects.nonNull( cached ) )
        {
            return cached;
        }

        FeatureGroup newInstance = new FeatureGroup( geometryGroup );
        FEATURE_GROUP_CACHE.put( geometryGroup, newInstance );
        return newInstance;
    }

    /**
     * @return the name
     */

    public String getName()
    {
        return this.geometryGroup.getRegionName();
    }

    /**
     * @return the wrapped feature tuples
     */

    public Set<FeatureTuple> getFeatures()
    {
        return this.features; //Immutable on construction
    }

    /**
     * @return the canonical geometry group
     */

    public GeometryGroup getGeometryGroup()
    {
        return this.geometryGroup;
    }

    /**
     * @return whether the feature group contains precisely one feature tuple
     */

    public boolean isSingleton()
    {
        return this.features.size() == 1;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }

        if ( ! ( o instanceof FeatureGroup in ) )
        {
            return false;
        }

        return Objects.equals( in.getGeometryGroup(), this.getGeometryGroup() );
    }

    @Override
    public int hashCode()
    {
        return this.getGeometryGroup().hashCode();
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
        return MessageUtilities.compare( this.geometryGroup, o.getGeometryGroup() );
    }

    /**
     * Hidden constructor.
     * @param geometryGroup the geometry group
     * @throws NullPointerException if the features are null
     * @throws IllegalArgumentException if the features are empty or the group name exceeds the maximum length
     */

    private FeatureGroup( GeometryGroup geometryGroup )
    {
        Objects.requireNonNull( geometryGroup );

        if ( geometryGroup.getGeometryTuplesCount() == 0 )
        {
            throw new IllegalArgumentException( "A feature group requires one or more features." );
        }

        if ( !geometryGroup.getRegionName().isBlank()
             && geometryGroup.getRegionName().length() > FeatureGroup.MAXIMUM_NAME_LENGTH )
        {
            throw new IllegalArgumentException( "A feature group name cannot be longer than " +
                                                FeatureGroup.MAXIMUM_NAME_LENGTH
                                                + " characters. The supplied name is too long, please shorten the name"
                                                + "and try again: "
                                                + geometryGroup.getRegionName()
                                                + "." );
        }

        this.geometryGroup = geometryGroup;
        SortedSet<FeatureTuple> innerFeatures = geometryGroup.getGeometryTuplesList()
                                                             .stream()
                                                             .map( FeatureTuple::of )
                                                             .collect( Collectors.toCollection( TreeSet::new ) );
        this.features = Collections.unmodifiableSortedSet( innerFeatures );

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Created a feature group with name '{}' and features {}.",
                          this.getName(),
                          this.getFeatures() );
        }
    }

}
