package wres.control;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import wres.config.generated.Feature;
import wres.io.config.ConfigHelper;

/**
 * Records the completion state of one {@link Feature}. 
 * 
 * See {@link FeatureReport} for a report on the completion state of one or more features.
 */

class FeatureProcessingResult
{
    private final Feature feature;
    private final boolean hadData;
    private final Throwable cause;
    private final Set<Path> pathsWrittenTo;

    FeatureProcessingResult( Feature feature,
                             boolean hadData,
                             Throwable cause,
                             Set<Path> pathsWrittenTo )
    {
        Objects.requireNonNull( feature );
        this.feature = feature;
        this.hadData = hadData;
        this.cause = cause;
        this.pathsWrittenTo = Collections.unmodifiableSet( pathsWrittenTo );
    }

    Feature getFeature()
    {
        return this.feature;
    }

    boolean hadData()
    {
        return this.hadData;
    }

    Throwable getCause()
    {
        return this.cause;
    }

    Set<Path> getPathsWrittenTo()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    @Override
    public String toString()
    {
        if ( hadData() )
        {
            return "Feature "
                   + ConfigHelper.getFeatureDescription( this.getFeature() )
                   + " had data.";
        }
        else
        {
            return "Feature "
                   + ConfigHelper.getFeatureDescription( this.getFeature() )
                   + " had no data: "
                   + this.getCause();
        }
    }
}
