package wres.datamodel.space;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import net.jcip.annotations.Immutable;

/**
 * Utility class for discovering correlations between features. Currently assumes a one:one relationship between 
 * features within a feature tuple. If this assumption is relaxed, the behavior here will need to be relaxed too. See 
 * #82867.
 * 
 * @author James Brown
 */

@Immutable
public class FeatureCorrelator
{

    /** The feature tuples mapped against the left features as keys. **/
    private final Map<Feature, FeatureTuple> featureTuplesByLeft;

    /** The left features mapped against the right features as keys. **/
    private final Map<Feature, Feature> leftFeaturesByRight;

    /** The left features mapped against the baseline features as keys. **/
    private final Map<Feature, Feature> leftFeaturesByBaseline;

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

    public Feature getLeftForRightFeature( Feature rightFeature )
    {
        Objects.requireNonNull( rightFeature );

        return this.leftFeaturesByRight.get( rightFeature );
    }

    /**
     * Returns the left feature correlated with the prescribed baseline feature or null if no left feature exists.
     * @param baselineFeature the baseline feature, not null
     * @return the left feature or null
     * @throws NullPointerException if the baselineFeature is null
     */

    public Feature getLeftForBaselineFeature( Feature baselineFeature )
    {
        Objects.requireNonNull( baselineFeature );

        return this.leftFeaturesByBaseline.get( baselineFeature );
    }

    /**
     * Returns the feature tuple correlated with the prescribed left feature or null if no feature tuple exists.
     * @param leftFeature the left feature, not null
     * @return the correlated feature tuple or null
     * @throws NullPointerException if the leftFeature is null
     */

    public FeatureTuple getFeatureTupleForLeftFeature( Feature leftFeature )
    {
        Objects.requireNonNull( leftFeature );

        return this.featureTuplesByLeft.get( leftFeature );
    }

    /**
     * Returns the left features mapped against the right features as keys.
     * @return the left features by right features
     */

    public Map<Feature, Feature> getLeftByRightFeatures()
    {
        // Immutable on construction
        return this.leftFeaturesByRight;
    }

    /**
     * Returns the left features mapped against the baseline features as keys.
     * @return the left features by baseline features
     */

    public Map<Feature, Feature> getLeftByBaselineFeatures()
    {
        // Immutable on construction
        return this.leftFeaturesByBaseline;
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
        this.featureTuplesByLeft = features.stream()
                                           .collect( Collectors.toUnmodifiableMap( FeatureTuple::getLeft,
                                                                                   Function.identity() ) );
    }

    /**
     * @param features the features
     * @return a mapping of the features
     */

    private Map<Feature, Feature> getFeatureMap( Set<FeatureTuple> features,
                                                 Function<? super FeatureTuple, ? extends Feature> keyMapper,
                                                 Function<? super FeatureTuple, ? extends Feature> valueMapper )
    {
        return features.stream()
                       // Ignore pairs with null keys or values
                       .filter( next -> Objects.nonNull( keyMapper.apply( next ) )
                                        && Objects.nonNull( valueMapper.apply( next ) ) )
                       .collect( Collectors.toUnmodifiableMap( keyMapper, valueMapper ) );
    }
}
