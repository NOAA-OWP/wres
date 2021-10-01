package wres.pipeline;

import java.util.Objects;
import wres.config.generated.Feature;
import wres.datamodel.space.FeatureGroup;

/**
 * Records the completion of one {@link Feature}. 
 * 
 * See {@link FeatureReporter} for a report on the completion of a collection of one or more features.
 */

class FeatureProcessingResult
{
    private final FeatureGroup featureGroup;

    /**
     * Is <code>true</code> if statistics were produced for one of more pools, otherwise <code>false</code>.
     */
    private final boolean hasStatistics;

    FeatureProcessingResult( FeatureGroup featureGroup,
                             boolean hasStatistics )
    {
        Objects.requireNonNull( featureGroup );
        this.featureGroup = featureGroup;
        this.hasStatistics = hasStatistics;
    }

    FeatureGroup getFeatureGroup()
    {
        return this.featureGroup;
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
               + " produced statistics: "
               + this.hasStatistics();
    }
}
