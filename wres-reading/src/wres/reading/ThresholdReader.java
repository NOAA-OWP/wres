package wres.reading;

import java.util.Set;

import wres.config.components.FeatureAuthority;
import wres.config.components.Threshold;
import wres.config.components.ThresholdSource;

/**
 * An API for reading thresholds from sources, such as files and web services.
 * @author James Brown
 */
public interface ThresholdReader
{
    /**
     * Reads thresholds.
     * @param thresholdSource the threshold source declaration
     * @param featureNames the named features for which thresholds are required
     * @param featureAuthority the feature authority associated with the feature names
     * @return the thresholds mapped against features
     * @throws NullPointerException if any input is null
     */
    Set<Threshold> read( ThresholdSource thresholdSource,
                         Set<String> featureNames,
                         FeatureAuthority featureAuthority );
}
