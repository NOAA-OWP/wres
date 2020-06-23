package wres.io.thresholds;

import wres.config.FeaturePlus;
import wres.config.MetricConfigException;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.csv.CSVThresholdReader;
import wres.io.thresholds.wrds.WRDSReader;
import wres.system.SystemSettings;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ExternalThresholdReader {
    public ExternalThresholdReader(
            final SystemSettings systemSettings,
            final ProjectConfig projectConfig,
            final Set<Feature> features,
            final UnitMapper desiredMeasurementUnitConverter,
            final ThresholdBuilderCollection builders
    ) {
        this.projectConfig = projectConfig;
        this.features = features;
        this.systemSettings = systemSettings;
        this.desiredMeasurementUnitConverter = desiredMeasurementUnitConverter;
        this.sharedBuilders = builders;
    }

    public void read() {
        for (MetricsConfig config : this.projectConfig.getMetrics()) {
            for (ThresholdsConfig thresholdsConfig : this.getThresholds(config)) {
                Set<Feature> readFeatures = this.readThreshold(
                        thresholdsConfig,
                        DataFactory.getMetricsFromMetricsConfig(config, this.projectConfig)
                );
                this.recognizedFeatures.addAll(readFeatures);
            }
        }
    }

    public Collection<Feature> getRecognizedFeatures() {
        return Collections.unmodifiableCollection(this.recognizedFeatures);
    }

    private Set<ThresholdsConfig> getThresholds(MetricsConfig metrics) {
        Set<ThresholdsConfig> thresholdsWithSources = new HashSet<>();

        for (ThresholdsConfig thresholdsConfig : metrics.getThresholds()) {
            if (thresholdsConfig.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source) {
                thresholdsWithSources.add(thresholdsConfig);
            }
        }

        return thresholdsWithSources;
    }

    /**
     * Reads a {@link ThresholdsConfig} and returns a corresponding {@link Set} of external {@link Threshold}
     * by {@link FeaturePlus}.
     *
     * @param thresholdsConfig the threshold configuration
     * @param metrics the metrics to which the threshold applies
     * @throws MetricConfigException if the threshold could not be read
     * @throws NullPointerException if the threshold configuration is null or the metrics are null
     */

    private Set<Feature> readThreshold(ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics)
    {
        Objects.requireNonNull( thresholdsConfig, "Specify non-null threshold configuration." );

        Objects.requireNonNull( metrics, "Specify non-null metrics." );

        Set<Feature> recognizedFeatures = new HashSet<>();

        // Threshold type: default to probability
        final ThresholdConstants.ThresholdGroup thresholdGroup;
        if ( Objects.nonNull( thresholdsConfig.getType() ) )
        {
            thresholdGroup = DataFactory.getThresholdGroup( thresholdsConfig.getType() );
        }
        else {
            thresholdGroup = ThresholdConstants.ThresholdGroup.PROBABILITY;
        }

        try
        {
            Map<FeaturePlus, Set<Threshold>> readThresholds;

            ThresholdFormat format = ExternalThresholdReader.getThresholdFormat(thresholdsConfig);

            switch (format) {
                case CSV:
                    readThresholds = CSVThresholdReader.readThresholds(
                            this.systemSettings,
                            thresholdsConfig,
                            this.getSourceMeasurementUnit(thresholdsConfig),
                            this.desiredMeasurementUnitConverter
                    );
                    break;
                case WRDS:
                    readThresholds = WRDSReader.readThresholds(
                            this.systemSettings,
                            thresholdsConfig,
                            this.desiredMeasurementUnitConverter,
                            this.features.parallelStream().map(FeaturePlus::of).collect(Collectors.toSet())
                    );
                    break;
                default:
                    String message = "The threshold format of " + format.toString() + " is not supported.";
                    throw new IllegalArgumentException(message);
            }

            // Add the thresholds for each feature
            for ( Map.Entry<FeaturePlus, Set<Threshold>> thresholds : readThresholds.entrySet() )
            {
                recognizedFeatures.add(thresholds.getKey().getFeature());

                for(MetricConstants metricName : metrics) {
                    for (Threshold threshold : thresholds.getValue()) {
                        // This employs the FeaturePlus; this will eventually devolve into just a Feature
                        this.sharedBuilders.addThreshold(
                                thresholds.getKey().getFeature(),
                                thresholdGroup,
                                metricName,
                                threshold
                        );
                    }
                }
            }

            return recognizedFeatures;
        }
        catch ( IOException e )
        {
            throw new MetricConfigException( thresholdsConfig, "Failed to read the comma separated thresholds.", e );
        }
    }

    public static ThresholdFormat getThresholdFormat(ThresholdsConfig config) {
        return ((ThresholdsConfig.Source)config.getCommaSeparatedValuesOrSource()).getFormat();
    }

    private MeasurementUnit getDesiredMeasurementUnit() {
        MeasurementUnit measurementUnit = null;

        if (this.projectConfig.getPair().getUnit() != null && !this.projectConfig.getPair().getUnit().isBlank()) {
            measurementUnit = MeasurementUnit.of(this.projectConfig.getPair().getUnit());
        }

        return measurementUnit;
    }

    private MeasurementUnit getSourceMeasurementUnit(ThresholdsConfig config) {
        MeasurementUnit measurementUnit;

        ThresholdsConfig.Source source = (ThresholdsConfig.Source)config.getCommaSeparatedValuesOrSource();

        if (source.getUnit() != null && !source.getUnit().isBlank()) {
            measurementUnit = MeasurementUnit.of(source.getUnit());
        }
        else
        {
            measurementUnit = this.getDesiredMeasurementUnit();
        }

        return measurementUnit;
    }

    private final ProjectConfig projectConfig;
    private final SystemSettings systemSettings;
    private final Set<Feature> features;
    private final Set<Feature> recognizedFeatures = new HashSet<>();
    private final UnitMapper desiredMeasurementUnitConverter;
    private final ThresholdBuilderCollection sharedBuilders;
}
