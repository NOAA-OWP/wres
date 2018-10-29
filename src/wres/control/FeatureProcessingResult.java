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
 * See {@link FeatureReport} for a report on the completion of a collection of one or more features.
 */

class FeatureProcessingResult
{
    private final Feature feature;
    private final Set<Path> pathsWrittenTo;

    FeatureProcessingResult( Feature feature,
                             Set<Path> pathsWrittenTo )
    {
        Objects.requireNonNull( feature );
        this.feature = feature;
        this.pathsWrittenTo = Collections.unmodifiableSet( pathsWrittenTo );
    }

    Feature getFeature()
    {
        return this.feature;
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    @Override
    public String toString()
    {
        return "Feature "
               + ConfigHelper.getFeatureDescription( this.getFeature() )
               + " succeeded.";
    }
}
