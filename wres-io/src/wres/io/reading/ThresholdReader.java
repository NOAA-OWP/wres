package wres.io.reading;

import java.util.Set;

import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdSource;
import wres.datamodel.units.UnitMapper;

/**
 * An API for reading thresholds from sources, such as files and web services.
 * @author James Brown
 */
public interface ThresholdReader
{
    /**
     * Reads thresholds.
     * @param thresholdSource the threshold source declaration
     * @param unitMapper a unit mapper to translate and set threshold units
     * @param featureNames the named features for which thresholds are required
     * @param featureAuthority the feature authority associated with the feature names
     * @return the thresholds mapped against features
     * @throws NullPointerException if any input is null
     */
    Set<Threshold> read( ThresholdSource thresholdSource,
                         UnitMapper unitMapper,
                         Set<String> featureNames,
                         FeatureAuthority featureAuthority );
}
