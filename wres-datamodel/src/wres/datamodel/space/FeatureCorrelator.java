package wres.datamodel.space;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;

/**
 * Utility class for discovering correlations between features. Allows a one-to-many relationship between features, 
 * i.e., admits duplicates on one or more sides (but not all sides). 
 * 
 * @author James Brown
 */

@Immutable
public class FeatureCorrelator
{
    /** The feature tuples mapped against the left features as keys. **/
    private final Map<Feature, Set<FeatureTuple>> featureTuplesByLeft;

    /** The left features mapped against the right features as keys. **/
    private final Map<Feature, Set<Feature>> leftFeaturesByRight;

    /** The left features mapped against the baseline features as keys. **/
    private final Map<Feature, Set<Feature>> leftFeaturesByBaseline;

    /**
     * Creates an instance with the specified features.
     * @param features the features
     * @return an instance
     * @throws NullPointerException if the features is null 
     */

    public static FeatureCorrelator of( Set<FeatureTuple> features )
    {
        return new FeatureCorrelator( features );
    }

    /**
     * Returns the left feature correlated with the prescribed right feature or null if no left feature exists
     * @param rightFeature the right feature, not null
     * @return the left feature or null
     * @throws NullPointerException if the rightFeature is null
     */

    public Set<Feature> getLeftForRightFeature( Feature rightFeature )
    {
        Objects.requireNonNull( rightFeature );

        return this.getOrEmptySet( this.leftFeaturesByRight, rightFeature );
    }

    /**
     * Returns the left feature correlated with the prescribed baseline feature or null if no left feature exists.
     * @param baselineFeature the baseline feature, not null
     * @return the left feature or null
     * @throws NullPointerException if the baselineFeature is null
     */

    public Set<Feature> getLeftForBaselineFeature( Feature baselineFeature )
    {
        Objects.requireNonNull( baselineFeature );

        return this.getOrEmptySet( this.leftFeaturesByBaseline, baselineFeature );
    }

    /**
     * Returns the feature tuple correlated with the prescribed left feature or null if no feature tuple exists.
     * @param leftFeature the left feature, not null
     * @return the correlated feature tuple or null
     * @throws NullPointerException if the leftFeature is null
     */

    public Set<FeatureTuple> getFeatureTuplesForLeftFeature( Feature leftFeature )
    {
        Objects.requireNonNull( leftFeature );

        return this.getOrEmptySet( this.featureTuplesByLeft, leftFeature );
    }

    /**
     * Helper that returns the empty set if the key doesn't exist in the map, otherwise the mapped value.
     * @param <T> the value type
     * @param map the map
     * @param key the map key
     * @return the mapped value or empty set
     */

    private <T> Set<T> getOrEmptySet( Map<Feature, Set<T>> map, Feature key )
    {
        if ( !map.containsKey( key ) )
        {
            return Collections.emptySet();
        }

        return map.get( key );
    }

    /**
     * Creates an instance.
     * @param features the features
     * @throws NullPointerException if the input is null
     */
    private FeatureCorrelator( Set<FeatureTuple> features )
    {
        Objects.requireNonNull( features );

        this.leftFeaturesByRight = this.getFeatureMap( features, FeatureTuple::getRight, FeatureTuple::getLeft );
        this.leftFeaturesByBaseline = this.getFeatureMap( features,
                                                          FeatureTuple::getBaseline,
                                                          FeatureTuple::getLeft );
        this.featureTuplesByLeft = this.getFeatureMap( features, FeatureTuple::getLeft, next -> next );
    }

    /**
     * @param features the features
     * @return a mapping of the features
     */

    private <S, T> Map<T, Set<S>> getFeatureMap( Set<FeatureTuple> features,
                                                 Function<? super FeatureTuple, ? extends T> keyMapper,
                                                 Function<? super FeatureTuple, ? extends S> valueMapper )
    {
        // Create the union in an unmodifiable set
        BinaryOperator<Set<S>> merger = ( a, b ) -> {
            Set<S> newSet = new HashSet<>();
            newSet.addAll( a );
            newSet.addAll( b );
            return Collections.unmodifiableSet( newSet );
        };

        return features.stream()
                       // Ignore pairs with null keys or values
                       .filter( next -> Objects.nonNull( keyMapper.apply( next ) )
                                        && Objects.nonNull( valueMapper.apply( next ) ) )
                       .collect( Collectors.toUnmodifiableMap( keyMapper::apply,
                                                               next -> Set.of( valueMapper.apply( next ) ),
                                                               merger ) );
    }
}
