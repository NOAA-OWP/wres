package wres.control;

import java.util.Objects;
import wres.config.generated.Feature;
import wres.datamodel.FeatureTuple;

/**
 * Records the completion of one {@link Feature}. 
 * 
 * See {@link FeatureReporter} for a report on the completion of a collection of one or more features.
 */

class FeatureProcessingResult
{
    private final FeatureTuple feature;

    /**
     * Is <code>true</code> if statistics were produced for one of more pools, otherwise <code>false</code>.
     */
    private final boolean hasStatistics;

    FeatureProcessingResult( FeatureTuple feature,
                             boolean hasStatistics )
    {
        Objects.requireNonNull( feature );
        this.feature = feature;
        this.hasStatistics = hasStatistics;
    }

    FeatureTuple getFeature()
    {
        return this.feature;
    }

    /**
     * Returns <code>true</code> if statistics were produced for one of more pools, otherwise <code>false</code>.
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
        return "Feature "
               + this.getFeature()
               + " produced statistics: "
               + this.hasStatistics();
    }
}
