package wres.io.thresholds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.io.retrieval.UnitMapper;
import wres.system.SystemSettings;

import java.util.*;
import java.util.stream.Collectors;

public class ThresholdReader {
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdReader.class );

    private final SystemSettings systemSettings;
    private final ProjectConfig projectConfig;
    private final UnitMapper desiredMeasurementUnitConverter;
    private final Set<FeatureTuple> features;
    private final Set<String> featureNames;
    private final Set<FeatureTuple> encounteredFeatures = new HashSet<>();
    private final ThresholdBuilderCollection builders = new ThresholdBuilderCollection();
    private final LeftOrRightOrBaseline useLeftOrRightOrBaseline;

    public ThresholdReader(
            final SystemSettings settings,
            final ProjectConfig projectConfig,
            final UnitMapper unitMapper,
            final Set<FeatureTuple> features,
            final LeftOrRightOrBaseline useLeftOrRightOrBaseline
    ) {
        this.systemSettings = settings;
        this.projectConfig = projectConfig;
        this.desiredMeasurementUnitConverter = unitMapper;
        this.features = features;
        this.useLeftOrRightOrBaseline = useLeftOrRightOrBaseline;
        Set<String> names = features.stream()
                                    .map( t -> t.getNameFor( useLeftOrRightOrBaseline ) )
                                    .collect( Collectors.toSet() );
        this.featureNames = Collections.unmodifiableSet( names );
        this.builders.initialize( this.featureNames );
    }

    public Map<FeatureTuple, ThresholdsByMetric> read() {
        ExternalThresholdReader externalReader = new ExternalThresholdReader(
                this.systemSettings,
                this.projectConfig,
                this.featureNames,
                this.desiredMeasurementUnitConverter,
                this.builders
        );

        InBandThresholdReader inBandReader = new InBandThresholdReader(this.projectConfig, this.builders);

        externalReader.read();
        inBandReader.read();

        for ( String feature : externalReader.getRecognizedFeatures() ) {
            this.registerFeature( feature );
        }

        this.builders.addAllDataThresholds(projectConfig);

        Map<String, ThresholdsByMetric> byName = this.builders.build();

        // At this point we have the low-level thresholds with Strings for names
        // but we need to have thresholds by FeatureTuple. The lower level stuff
        // only needed names, but here we are going back to higher level. If you
        // want, you can refactor to spread the FeatureTuple everywhere too. The
        // lowest level, though, is to have a name for a feature when reading
        // thresholds.
        Map<FeatureTuple, ThresholdsByMetric> byTuple = new HashMap<>( byName.size() );

        for ( FeatureTuple featureTuple : this.features )
        {
            String featureName = featureTuple.getNameFor( this.useLeftOrRightOrBaseline );
            ThresholdsByMetric thresholdsByMetric = byName.get( featureName );
            byTuple.put( featureTuple, thresholdsByMetric );
        }

        return Collections.unmodifiableMap( byTuple );
    }


    public Set<FeatureTuple> getEvaluatableFeatures() {
        if (this.encounteredFeatures.size() == 0) {
            return this.features;
        }

        LOGGER.debug( "Attempting to reconcile the {} features to evaluate with the {} features for which external "
                        + "thresholds are available.",
                this.features.size(),
                this.builders.featureCount() );


        // Iterate the features to evaluate, filtering any for which external thresholds are not available
        Set<FeatureTuple> missingThresholds = this.features
                .stream()
                .filter(feature -> !this.encounteredFeatures.contains(feature))
                .collect(Collectors.toSet());

        if ( !missingThresholds.isEmpty() && LOGGER.isWarnEnabled() )
        {
            StringJoiner joiner = new StringJoiner( ", " );

            for ( FeatureTuple feature : missingThresholds )
            {
                joiner.add( feature.toString() );
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

    private void registerFeature( final String feature )
    {
        for ( FeatureTuple featureTuple : this.features )
        {
            // When the name for the given l/r/b matches the threshold name,
            // add to set of encountered FeatureTuple instances.
            if ( featureTuple.getNameFor( this.useLeftOrRightOrBaseline )
                             .equals( feature ) )
            {
                this.encounteredFeatures.add( featureTuple );
            }
        }
    }
}
