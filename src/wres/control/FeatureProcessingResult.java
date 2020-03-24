package wres.control;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;

/**
 * Records the completion of one {@link Feature}. 
 * 
 * See {@link FeatureReporter} for a report on the completion of a collection of one or more features.
 */

class FeatureProcessingResult
{
    private final Feature feature;
    private final Set<Path> pathsWrittenTo;

    /**
     * Is <code>true</code> if statistics were produced for one of more pools, otherwise <code>false</code>.
     */
    private final boolean hasStatistics;

    FeatureProcessingResult( Feature feature,
                             Set<Path> pathsWrittenTo,
                             boolean hasStatistics )
    {
        Objects.requireNonNull( feature );
        this.feature = feature;
        this.pathsWrittenTo = Collections.unmodifiableSet( pathsWrittenTo );
        this.hasStatistics = hasStatistics;
    }

    Feature getFeature()
    {
        return this.feature;
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
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
               + ConfigHelper.getFeatureDescription( this.getFeature() )
               + " produced statistics: "
               + this.hasStatistics()
               + "; and created these paths: "
               + this.getPathsWrittenTo();
    }
}
