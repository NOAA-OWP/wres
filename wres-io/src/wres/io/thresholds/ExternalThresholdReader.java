package wres.io.thresholds;

import wres.config.MetricConfigException;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.csv.CSVThresholdReader;
import wres.io.thresholds.wrds.WRDSReader;
import wres.system.SystemSettings;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ExternalThresholdReader {
    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final Set<FeatureTuple> features;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final ThresholdBuilderCollection sharedBuilders;
    private final Set<FeatureTuple> recognizedFeatures = new HashSet<>();

    public ExternalThresholdReader(
            final SystemSettings systemSettings,
            final ProjectConfig projectConfig,
            final Set<FeatureTuple> features,
            final UnitMapper desiredMeasurementUnitConverter,
            final ThresholdBuilderCollection builders
    ) {
        this.systemSettings = systemSettings;
        this.projectConfig = projectConfig;
        this.features = features;
        this.desiredMeasurementUnitConverter = desiredMeasurementUnitConverter;
        this.sharedBuilders = builders;
    }

    public void read() {
        for ( MetricsConfig config : this.projectConfig.getMetrics() )
        {
            for ( ThresholdsConfig thresholdsConfig : this.getThresholds( config) )
            {
                Set<FeatureTuple> readFeatures = this.readThreshold(
                        thresholdsConfig,
                        DataFactory.getMetricsFromMetricsConfig(config, this.projectConfig)
                );
                this.recognizedFeatures.addAll(readFeatures);
            }
        }
    }

    public Set<FeatureTuple> getRecognizedFeatures()
    {
        return Collections.unmodifiableSet( this.recognizedFeatures );
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
     * Reads a {@link ThresholdsConfig} and returns a corresponding {@link Set} of external {@link ThresholdOuter}
     * by {@link String}.
     *
     * @param thresholdsConfig the threshold configuration
     * @param metrics the metrics to which the threshold applies
     * @throws MetricConfigException if the threshold could not be read
     * @throws NullPointerException if the threshold configuration is null or the metrics are null
     */

    private Set<FeatureTuple> readThreshold( ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( thresholdsConfig, "Specify non-null threshold configuration." );

        Objects.requireNonNull( metrics, "Specify non-null metrics." );

        ThresholdsConfig.Source source = (ThresholdsConfig.Source)thresholdsConfig.getCommaSeparatedValuesOrSource();
        LeftOrRightOrBaseline tupleSide = source.getFeatureNameFrom();

        Set<FeatureTuple> recognizedFeatures = new HashSet<>();

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
            Map<String, Set<ThresholdOuter>> readThresholds;
            ThresholdFormat format = ExternalThresholdReader.getThresholdFormat( thresholdsConfig );

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
                            this.features.stream().map(tuple -> tuple.getNameFor(tupleSide)).collect(Collectors.toSet())
                    );
                    break;
                default:
                    String message = "The threshold format of " + format.toString() + " is not supported.";
                    throw new IllegalArgumentException(message);
            }

            // Add the thresholds for each feature
            for ( Map.Entry<String, Set<ThresholdOuter>> thresholds : readThresholds.entrySet() )
            {
                Optional<FeatureTuple> possibleFeature = this.features.stream()
                        .filter(tuple -> tuple.getNameFor(tupleSide).equals(thresholds.getKey()))
                        .findFirst();

                if (possibleFeature.isEmpty())
                {
                    continue;
                }

                FeatureTuple feature = possibleFeature.get();
                recognizedFeatures.add( feature );

                for(MetricConstants metricName : metrics) {
                    for (ThresholdOuter threshold : thresholds.getValue()) {
                        // This employs the FeaturePlus; this will eventually devolve into just a Feature
                        this.sharedBuilders.addThreshold(
                                feature,
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
            throw new MetricConfigException( "Failed to read the external thresholds.", e );
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
}
