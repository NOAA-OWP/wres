package wres.control;

import java.util.Objects;

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

    FeatureProcessingResult( Feature feature,
                             boolean hadData,
                             Throwable cause )
    {
        Objects.requireNonNull( feature );
        this.feature = feature;
        this.hadData = hadData;
        this.cause = cause;
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
