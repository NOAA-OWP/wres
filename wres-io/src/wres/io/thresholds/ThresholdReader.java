package wres.io.thresholds;

import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.units.UnitMapper;

import java.util.*;

/**
 * Threshold reader.
 */
public class ThresholdReader
{
    private final ProjectConfig projectConfig;
    private final MetricsConfig metricsConfig;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final Set<FeatureTuple> features;
    private final Set<FeatureTuple> encounteredFeatures = new HashSet<>();

    /**
     * Creates an instance.
     * @param projectConfig the project declaration
     * @param metricsConfig the metrics declaration
     * @param unitMapper the unit mapper
     * @param features the features
     */
    public ThresholdReader( ProjectConfig projectConfig,
                            MetricsConfig metricsConfig,
                            UnitMapper unitMapper,
                            Set<FeatureTuple> features )
    {
        this.projectConfig = projectConfig;
        this.metricsConfig = metricsConfig;
        this.desiredMeasurementUnitConverter = unitMapper;
        this.features = features;
    }

    /**
     * Reads the thresholds.
     * @return the thresholds by feature
     */
    public Map<FeatureTuple, Set<ThresholdOuter>> read()
    {
        ExternalThresholdReader externalReader = new ExternalThresholdReader( this.projectConfig,
                                                                              this.metricsConfig,
                                                                              this.features,
                                                                              this.desiredMeasurementUnitConverter );

        MeasurementUnit units =
                MeasurementUnit.of( this.desiredMeasurementUnitConverter.getDesiredMeasurementUnitName() );
        InBandThresholdReader inBandReader = new InBandThresholdReader( this.projectConfig,
                                                                        this.metricsConfig,
                                                                        units );

        Map<FeatureTuple, Set<ThresholdOuter>> externalThresholds = externalReader.read();
        Set<ThresholdOuter> declaredThresholds = inBandReader.read();

        Map<FeatureTuple, Set<ThresholdOuter>> combinedThresholds = new HashMap<>();
        for( FeatureTuple nextFeature : this.features )
        {
            if( externalThresholds.containsKey( nextFeature ) )
            {
                Set<ThresholdOuter> nextCombined = new HashSet<>( externalThresholds.get( nextFeature ) );
                nextCombined.addAll( declaredThresholds );
                combinedThresholds.put( nextFeature, nextCombined );
            }
            else
            {
                combinedThresholds.put( nextFeature, declaredThresholds );
            }
        }

        this.encounteredFeatures.addAll( externalReader.getRecognizedFeatures() );

        return Collections.unmodifiableMap( combinedThresholds );
    }

    /**
     * @return the set of features with thresholds, not including the "all data" threshold, which is added for every
     * feature.
     */

    public Set<FeatureTuple> getEvaluatableFeatures()
    {
        return Collections.unmodifiableSet( this.encounteredFeatures );
    }

}
