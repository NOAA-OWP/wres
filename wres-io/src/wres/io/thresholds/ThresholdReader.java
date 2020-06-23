package wres.io.thresholds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.*;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.io.config.ConfigHelper;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

import java.util.*;
import java.util.stream.Collectors;

public class ThresholdReader {
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdReader.class );

    public ThresholdReader(
            final SystemSettings settings,
            final ProjectConfig projectConfig,
            final UnitMapper unitMapper,
            final Set<Feature> features
    ) {
        this.systemSettings = settings;
        this.projectConfig = projectConfig;
        this.desiredMeasurementUnitConverter = unitMapper;
        this.features = features;

        this.builders.initialize(this.features);
    }

    public Map<Feature, ThresholdsByMetric> read() {
        ExternalThresholdReader externalReader = new ExternalThresholdReader(
                this.systemSettings,
                this.projectConfig,
                this.features,
                this.desiredMeasurementUnitConverter,
                this.builders
        );

        InBandThresholdReader inBandReader = new InBandThresholdReader(this.projectConfig, this.builders);

        externalReader.read();
        inBandReader.read();

        for (Feature feature : externalReader.getRecognizedFeatures()) {
            this.registerFeature(feature);
        }

        this.builders.addAllDataThresholds(projectConfig);

        return this.builders.build();
    }

    public Set<Feature> getEvaluatableFeatures() {
        if (this.encounteredFeatures.size() == 0) {
            return this.features;
        }

        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                        + "thresholds are available.",
                this.features.size(),
                this.builders.featureCount() );


        // Iterate the features to evaluate, filtering any for which external thresholds are not available
        Set<Feature> missingThresholds = this.features
                .stream()
                .filter(feature -> !this.encounteredFeatures.contains(feature))
                .collect(Collectors.toSet());

        if ( !missingThresholds.isEmpty() && LOGGER.isWarnEnabled() )
        {
            StringJoiner joiner = new StringJoiner( ", " );

            for ( Feature feature : missingThresholds )
            {
                String description = ConfigHelper.getFeatureDescription( feature );
                joiner.add( description );
            }

            LOGGER.warn( "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
                    "While attempting to reconcile the features to ",
                    "evaluate with the features for which thresholds ",
                    "are available, found ",
                    this.features.size(),
                    " features to evaluate and ",
                    this.builders.featureCount(),
                    " features for which thresholds are available, but ",
                    missingThresholds.size(),
                    " features for which thresholds could not be ",
                    "reconciled with features to evaluate. Features without ",
                    "thresholds will be skipped. If the number of features ",
                    "without thresholds is larger than expected, ensure ",
                    "that the type of feature name (featureType) is properly ",
                    "declared for the external source of thresholds. The ",
                    "features without thresholds are: ",
                    joiner,
                    "." );
        }

        LOGGER.info( "Discovered {} features to evaluate for which external thresholds were available and {} "
                        + "features with external thresholds that could not be evaluated (e.g., because there was "
                        + "no data for these features).",
                this.encounteredFeatures.size(),
                this.builders.featureCount() - this.encounteredFeatures.size() );

        return Collections.unmodifiableSet(this.encounteredFeatures);
    }

    private void registerFeature(final Feature feature) {
        for (Feature storedFeature : this.features) {
            String featureLID = feature.getLocationId();
            String featureGage = feature.getGageId();
            CoordinateSelection featureCoordinates = feature.getCoordinate();
            Long featureComid = feature.getComid();

            String storedFeatureLID = storedFeature.getLocationId();
            String storedFeatureGage = storedFeature.getGageId();
            CoordinateSelection storedFeatureCoordinates = storedFeature.getCoordinate();
            Long storedFeatureComid = storedFeature.getComid();

            boolean lidsMatch = featureLID != null && featureLID.equals(storedFeatureLID);
            boolean gagesMatch = featureGage != null && featureGage.equals(storedFeatureGage);
            boolean coordinatesMatch = featureCoordinates != null && featureCoordinates.equals(storedFeatureCoordinates);
            boolean comidsMatch = featureComid != null && featureComid.equals(storedFeatureComid);

            if (lidsMatch || gagesMatch || coordinatesMatch || comidsMatch) {
                this.encounteredFeatures.add(storedFeature);
                break;
            }
        }
    }

    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final Set<Feature> features;
    private final Set<Feature> encounteredFeatures = new HashSet<>();
    private final ThresholdBuilderCollection builders = new ThresholdBuilderCollection();
}
