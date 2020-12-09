package wres.io.thresholds;

import wres.config.MetricConfigException;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.io.geography.wrds.WrdsLocation;
import wres.io.retrieval.UnitMapper;
import wres.io.thresholds.csv.CSVThresholdReader;
import wres.io.thresholds.wrds.WRDSReader;
import wres.system.SystemSettings;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
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
                    Map<WrdsLocation, Set<ThresholdOuter>> wrdsThresholds = WRDSReader.readThresholds(
                            this.systemSettings,
                            thresholdsConfig,
                            this.desiredMeasurementUnitConverter,
                            this.features.stream().map(tuple -> tuple.getNameFor(tupleSide)).collect(Collectors.toSet())
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

            // Now that we have mappings between location identifiers and their thresholds,
            // try to match those up with our features
            for ( Map.Entry<String, Set<ThresholdOuter>> thresholds : readThresholds.entrySet() )
            {
                // Try to find one of our configured features whose side matches what we were able to pluck out
                // from our threshold requests
                Optional<FeatureTuple> possibleFeature = this.features.stream()
                        .filter(tuple -> tuple.getNameFor(tupleSide).equals(thresholds.getKey()))
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

        // If the user declares "This is a usgs site code", we want to default to that
        FeatureDimension dimension = dataSourceConfig.getFeatureDimension();

        // If the user DOESN'T say what the dimension is or gives a vague definition, we need to explore the
        // declaration and try to pull out any other hints
        if (dimension == null || dimension == FeatureDimension.CUSTOM) {
            FeatureDimension foundDimension = null;

            // Since a DataSourceConfig must have 1+ definitions, we need to look through all of them to first get an
            // idea of what the identifiers are and second make sure there aren't any conflicts
            for (DataSourceConfig.Source source : dataSourceConfig.getSource()) {
                // First declare the format, interface, and address so as to make the coming if statements less
                // ridiculous
                String sourceFormat = "";

                if (source.getFormat() != null) {
                    sourceFormat = source.getFormat().value().toLowerCase();
                }

                String sourceInterface = "";

                if (source.getInterface() != null) {
                    sourceInterface = source.getInterface().value().toLowerCase();
                }

                String address = "";

                if (source.getValue() != null) {
                    address = source.getValue().toString().toLowerCase();
                }

                /*
                    This is hacky, but assumptions need to be made to make these leaps:

                    nwm_feature_id:

                    - Our only file based netcdf data at the moment is indexed via feature_id, so declaring netcdf
                        (for the time being) means that we're going to use nwm_feature_id
                    - If we're using an interface that contains 'nwm', we are definitely using data that is indexed via
                        the nwm_feature_id

                    usgs_site_code:

                    - Our only theoretical waterml input apes USGS data, so, if we define WaterML (for the time being),
                        locations will be identified via the USGS site code
                    - If we explicitly tell the system to utilize a USGS interface, we know that our location will be
                        identified through a USGS site code
                    - If the address the user declares contains the major portions of the NWIS URL, we know that we'll
                        be using the USGS site code
                            * There are two end points for NWIS - one they hope we use for small requests, one for
                                large - so we can't hard code it to the entire beginning of the URL

                     nws_lid:

                     - The nws_lid has been our standard so far and has been used in most, if not all, of the
                        user-tailored inputs, so we use it as our fallback in order to cover as many bases as possible.
                 */
                if (sourceFormat.equals("netcdf") || sourceInterface.contains("nwm")) {
                    if (foundDimension == null || foundDimension == FeatureDimension.NWM_FEATURE_ID) {
                        foundDimension = FeatureDimension.NWM_FEATURE_ID;
                    }
                    else {
                        throw new IllegalStateException(
                                "External threshold identifiers cannot be interpretted if the input data is both " +
                                        foundDimension + " and " + FeatureDimension.NWM_FEATURE_ID
                        );
                    }
                }
                else if (sourceFormat.equals("waterml") || sourceInterface.contains("usgs") || address.contains("usgs.gov/nwis")) {
                    if (foundDimension == null || foundDimension == FeatureDimension.USGS_SITE_CODE) {
                        foundDimension = FeatureDimension.USGS_SITE_CODE;
                    }
                    else {
                        throw new IllegalStateException(
                                "External threshold identifiers cannot be interpretted if the input data is both " +
                                        foundDimension + " and " + FeatureDimension.USGS_SITE_CODE
                        );
                    }
                }
                else {
                    if (foundDimension == null || foundDimension == FeatureDimension.NWS_LID) {
                        foundDimension = FeatureDimension.NWS_LID;
                    }
                    else {
                        throw new IllegalStateException(
                                "External threshold identifiers cannot be interpretted if the input data is both " +
                                        foundDimension + " and " + FeatureDimension.NWS_LID
                        );
                    }
                }
            }

            dimension = foundDimension;
        }

        // There's nothing more that we can do if we haven't been able to infer from the sources, so we just need
        // to error out
        if (dimension == null) {
            throw new IllegalStateException(
                    "The type of location identifier to use when interpreting WRDS responses could not be determined. " +
                            "Please supply the feature dimension on the " + side + " data source configuration.");

        }

        // Now that we know what dimension to use, we just have to return a function that will pluck the right
        // one off of the WrdsLocation
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
