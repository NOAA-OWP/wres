package wres.pipeline;

import java.util.Objects;
import java.util.Set;

import wres.config.generated.Feature;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;

/**
 * Records the completion of one {@link Feature}. 
 * 
 * See {@link FeatureReporter} for a report on the completion of a collection of one or more features.
 */

class FeatureProcessingResult
{
    /** The feature group. */
    private final FeatureGroup featureGroup;

    /** Features with one or more pools that contained one or more pairs. */
    private final Set<FeatureTuple> featuresWithData;
    
    /**
     * Is <code>true</code> if statistics were produced for one of more pools, otherwise <code>false</code>.
     */
    private final boolean hasStatistics;

    /**
     * @param featureGroup the feature group
     * @param featuresWithData the features that had data
     * @param hasStatistics is true if the group produced statistics
     */
    FeatureProcessingResult( FeatureGroup featureGroup,
                             Set<FeatureTuple> featuresWithData,
                             boolean hasStatistics )
    {
        Objects.requireNonNull( featureGroup );
        Objects.requireNonNull( featuresWithData );
        this.featureGroup = featureGroup;
        this.hasStatistics = hasStatistics;
        this.featuresWithData = featuresWithData;
    }

    /**
     * @return the feature group
     */
    FeatureGroup getFeatureGroup()
    {
        return this.featureGroup;
    }
    
    /**
     * @return the features with data
     */
    Set<FeatureTuple> getFeaturesWithData()
    {
        return this.featuresWithData;
    }

    /**
     * Returns <code>true</code> if statistics were produced for one or more pools, otherwise <code>false</code>.
     * 
     * @return true if statistics were produced, false if no statistics were produced
     */

    boolean hasStatistics()
    {
        return this.hasStatistics;
    }

    @Override
    public String toString()
    {
        return "Feature group "
               + this.getFeatureGroup()
               + " had data for these features, "
               + this.getFeaturesWithData()
               + ", which produced statistics: "
               + this.hasStatistics()
               +".";
    }
}
