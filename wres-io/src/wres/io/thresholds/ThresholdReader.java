package wres.io.thresholds;

import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

import java.util.*;

public class ThresholdReader
{
    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final MetricsConfig metricsConfig;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final Set<FeatureTuple> features;
    private final Set<FeatureTuple> encounteredFeatures = new HashSet<>();
    private final ThresholdBuilderCollection builders = new ThresholdBuilderCollection();

    public ThresholdReader(
                            final SystemSettings settings,
                            final ProjectConfig projectConfig,
                            final MetricsConfig metricsConfig,
                            final UnitMapper unitMapper,
                            final Set<FeatureTuple> features )
    {
        this.systemSettings = settings;
        this.projectConfig = projectConfig;
        this.metricsConfig = metricsConfig;
        this.desiredMeasurementUnitConverter = unitMapper;
        this.features = features;
        this.builders.initialize( this.features );
    }

    public Map<FeatureTuple, ThresholdsByMetric> read()
    {
        ExternalThresholdReader externalReader = new ExternalThresholdReader(
                                                                              this.systemSettings,
                                                                              this.projectConfig,
                                                                              this.metricsConfig,
                                                                              this.features,
                                                                              this.desiredMeasurementUnitConverter,
                                                                              this.builders );

        MeasurementUnit units =
                MeasurementUnit.of( this.desiredMeasurementUnitConverter.getDesiredMeasurementUnitName() );
        InBandThresholdReader inBandReader = new InBandThresholdReader( this.projectConfig,
                                                                        this.metricsConfig,
                                                                        this.builders,
                                                                        units );

        externalReader.read();
        inBandReader.read();

        this.encounteredFeatures.addAll( externalReader.getRecognizedFeatures() );

        this.builders.addAllDataThresholds( this.projectConfig, this.metricsConfig );

        return Collections.unmodifiableMap( this.builders.build() );
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
