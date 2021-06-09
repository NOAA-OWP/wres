package wres.io.thresholds;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.FeatureTuple;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.config.ConfigHelper;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.csv.CSVThresholdReader;
import wres.io.thresholds.wrds.GeneralWRDSReader;
import wres.system.SystemSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExternalThresholdReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalThresholdReader.class);

    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final MetricsConfig metricsConfig;
    private final Set<FeatureTuple> features;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final ThresholdBuilderCollection sharedBuilders;
    private final Set<FeatureTuple> recognizedFeatures = new HashSet<>();
    private final MeasurementUnit desiredMeasurementUnit;

    public ExternalThresholdReader(
                                    final SystemSettings systemSettings,
                                    final ProjectConfig projectConfig,
                                    final MetricsConfig metricsConfig,
                                    final Set<FeatureTuple> features,
                                    final UnitMapper desiredMeasurementUnitConverter,
                                    final ThresholdBuilderCollection builders )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( metricsConfig );
        Objects.requireNonNull( features );
        Objects.requireNonNull( desiredMeasurementUnitConverter );
        Objects.requireNonNull( builders );
        
        this.systemSettings = systemSettings;
        this.projectConfig = projectConfig;
        this.metricsConfig = metricsConfig;
        this.features = features;
        this.desiredMeasurementUnitConverter = desiredMeasurementUnitConverter;
        this.sharedBuilders = builders;
        this.desiredMeasurementUnit =
                MeasurementUnit.of( this.desiredMeasurementUnitConverter.getDesiredMeasurementUnitName() );
    }

    public void read()
    {       
        Set<MetricConstants> metrics = DataFactory.getMetricsFromMetricsConfig( this.metricsConfig, 
                                                                                this.projectConfig );
        
        for ( ThresholdsConfig thresholdsConfig : this.getThresholds( this.metricsConfig ) )
        {
            Set<FeatureTuple> readFeatures = this.readThreshold( thresholdsConfig, metrics );
            this.recognizedFeatures.addAll( readFeatures );
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
     * Reads a {@link ThresholdsConfig} and returns a corresponding {@link Set} of {@link FeatureTuple} that may be evaluated
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

        DataSourceConfig dataSourceConfig = ProjectConfigs.getDataSourceBySide( this.projectConfig, tupleSide);
        FeatureDimension featureDimension = ConfigHelper.getConcreteFeatureDimension(dataSourceConfig);

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

            // If we're going with NWS_LIDs with WRDS, we want to do equality checks on the strict handbook-5s.
            // Some RFC data append an added identifier to the end of their handbook 5, preventing a match, so we
            // want to roll with equivalence based on those first five characters. To achieve this, we use a custom
            // function for the equivalency checks in the coming loop rather than a strict String::equals
            BiPredicate<String, String> equalityCheck = String::equals;

            // Produce mappings between the identifiers for data that may be linked to the features to their thresholds
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
                    final Function<FeatureTuple, String> identifyFeatureName;

                    // Only 5 character handbook-5 ids are supported, so we need to ensure that feature names that
                    // are passed are 5 characters long, at most, when passing to the reader if the user indicates
                    // that it uses NWS LIDs
                    if (featureDimension == FeatureDimension.NWS_LID) {
                        identifyFeatureName = tuple ->
                                tuple.getNameFor(tupleSide)
                                        .substring(
                                                0,
                                                Math.min(tuple.getNameFor(tupleSide).length(), 5)
                                        );

                        // We want to mimic the five character equality check later on as well
                        equalityCheck = (first, second) ->
                                first != null
                                        && first.substring(0, Math.min(first.length(), 5)).equals(second);
                    }
                    else {
                        identifyFeatureName = tuple -> tuple.getNameFor(tupleSide);
                    }

                    //Naive toggle: use one of two versions depending on flag.
                    Map<WrdsLocation, Set<ThresholdOuter>> wrdsThresholds = GeneralWRDSReader.readThresholds(
                            this.systemSettings,
                            thresholdsConfig,
                            this.desiredMeasurementUnitConverter,
                            this.features.stream().map(identifyFeatureName).collect(Collectors.toSet())
                        );

                    // WRDS returns a series of identifiers that don't match to 'left' 'right', or 'baseline'.
                    // As a result, we have to look through the sources and find a consistent way to identify what id
                    // from WRDS matches with what ID from our tuples
                    final Function<WrdsLocation, String> extractLocationName = this.getFeatureIdentifier(tupleSide);

                    /*
                     *  In english:
                     *
                     *  Take the mapping of WrdsLocations to their thresholds and make a new map by mapping each
                     *  value to a string created by applying the created extraction function to the key
                     */
                    readThresholds = wrdsThresholds.entrySet()
                            .parallelStream()
                            .collect(
                                    Collectors.toUnmodifiableMap(
                                            entry -> extractLocationName.apply(entry.getKey()), Map.Entry::getValue
                                    )
                            );
                    break;
                default:
                    String message = "The threshold format of " + format.toString() + " is not supported.";
                    throw new IllegalArgumentException(message);
            }
            
            final BiPredicate<String, String> finalEqualityCheck = equalityCheck;

            // Now that we have mappings between location identifiers and their thresholds,
            // try to match those up with our features
            for ( Map.Entry<String, Set<ThresholdOuter>> thresholds : readThresholds.entrySet() )
            {
                final String locationIdentifier = thresholds.getKey();

                // Try to find one of our configured features whose side matches what we were able to pluck out
                // from our threshold requests
                Optional<FeatureTuple> possibleFeature = this.features.stream()
                        .filter(tuple -> finalEqualityCheck.test(tuple.getNameFor(tupleSide), locationIdentifier))
                        .findFirst();

                // If none were found, just move on. This might happen in the case where a CSV returns a mountain of
                // thresholds yet we only want a few
                if (possibleFeature.isEmpty())
                {
                    continue;
                }

                // Now that we know we have a match, add the feature to the list of features we know can be evaluated
                FeatureTuple feature = possibleFeature.get();
                recognizedFeatures.add( feature );

                // Now that we have the feature, the metrics, and the thresholds, we can now add them to a
                // greater collection for use later
                for(MetricConstants metricName : metrics) {
                    for (ThresholdOuter threshold : thresholds.getValue()) {
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

    /**
     * Creates a function to examine a WrdsLocation and extract the correct identifier
     *
     * @param side The side of the project's input to read identifiers from
     * @return a function that will retrieve the correct identifier from a WrdsLocation
     */
    private Function<WrdsLocation, String> getFeatureIdentifier(LeftOrRightOrBaseline side) {
        // Start out by getting the DataSourceConfig corresponding with the side; we'll need this in order to
        // try to figure out what field from the WrdsLocation we want to read from
        DataSourceConfig dataSourceConfig;

        switch (side) {
            case RIGHT:
                dataSourceConfig = this.projectConfig.getInputs().getRight();
                break;
            case BASELINE:
                dataSourceConfig = this.projectConfig.getInputs().getBaseline();
                break;
            default:
                dataSourceConfig = this.projectConfig.getInputs().getLeft();
        }

        FeatureDimension dimension = ConfigHelper.getConcreteFeatureDimension(dataSourceConfig);

        if (dimension == null || dimension == FeatureDimension.CUSTOM) {
            LOGGER.warn("A definitive feature dimension could not be determined for linking thresholds to features; " +
                    "defaulting to the NWS lid");
            return WrdsLocation::getNwsLid;
        }

        // Now that we know what dimension to use, we just have to return a function that will pluck the right
        // one off of the WrdsLocation. WRDS only supports three different formats and it's fairly obvious which
        // sources use NWM ids or USGS sites, not so much for NWS lids. Since what CAN use NWS lids is so vague,
        // we assume that as the base case.
        switch (dimension) {
            case NWM_FEATURE_ID:
                return WrdsLocation::getNwmFeatureId;
            case USGS_SITE_CODE:
                return WrdsLocation::getUsgsSiteCode;
            default:
                return WrdsLocation::getNwsLid;
        }
    }

    public static ThresholdFormat getThresholdFormat(ThresholdsConfig config) {
        return ((ThresholdsConfig.Source)config.getCommaSeparatedValuesOrSource()).getFormat();
    }

    private MeasurementUnit getDesiredMeasurementUnit() {
        return this.desiredMeasurementUnit;
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
