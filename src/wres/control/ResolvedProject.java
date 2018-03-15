package wres.control;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdsByType;

/**
 * Represents a project that has been "resolved", i.e. any kind of translation
 * from the ProjectConfig into WRES that has happened can be captured here.
 * The existence of this class is a possible solution to these discomforts:
 * 1) decomposed features which are disconnected from the original config
 * 2) project identity in the database (discovered post-ingest)
 *
 * Both the decomposed features and project identity are resolved outside of
 * the original project config, and need a home for sharing. This class might
 * be that home.
 *
 */
class ResolvedProject
{
    private static final Logger LOGGER
            = LoggerFactory.getLogger( ResolvedProject.class );

    private final ProjectConfigPlus projectConfigPlus;
    private final Set<FeaturePlus> decomposedFeatures;
    private final String projectIdentifier;
    private final Map<FeaturePlus, Map<MetricConfigName, ThresholdsByType>> thresholds;

    private ResolvedProject( ProjectConfigPlus projectConfigPlus,
                             Set<FeaturePlus> decomposedFeatures,
                             String projectIdentifier,
                             Map<FeaturePlus, Map<MetricConfigName, ThresholdsByType>> thresholds )
    {
        this.projectConfigPlus = projectConfigPlus;
        this.decomposedFeatures = Collections.unmodifiableSet( decomposedFeatures );
        this.projectIdentifier = projectIdentifier;
        this.thresholds = Collections.unmodifiableMap( thresholds );
    }

    static ResolvedProject of( ProjectConfigPlus projectConfigPlus,
                               Set<FeaturePlus> decomposedFeatures,
                               String projectIdentifier,
                               Map<FeaturePlus, Map<MetricConfigName, ThresholdsByType>> thresholds )
    {
        return new ResolvedProject( projectConfigPlus,
                                    decomposedFeatures,
                                    projectIdentifier,
                                    thresholds );

    }

    /**
     * Get the supplemented ProjectConfig, the ProjectConfigPlus
     * @return the ProjectConfigPlus
     */
    ProjectConfigPlus getProjectConfigPlus()
    {
        return this.projectConfigPlus;
    }

    /**
     * Get the ProjectConfig. ProjectConfig is used by most classes.
     * @return the original project config as specified by the user
     */

    ProjectConfig getProjectConfig()
    {
        return this.getProjectConfigPlus()
                   .getProjectConfig();
    }


    /**
     * Get the decomposed features that the system calculated from the given
     * project config.
     * In the future, hopefully we can figure out a different representation for
     * feature besides the object that implies something came from a config.
     * Once we figure that out, the return type will change.
     * @return a Set of features resolved from the project
     */

    Set<FeaturePlus> getDecomposedFeatures()
    {
        return this.decomposedFeatures;
    }


    /**
     * Get the wres identifier generated for this project.
     * As of 2018-03-14 this is anticipated to be the md5sum generated at the
     * end of ingest that incorporates both the project configuration and the
     * sources that were actually ingested for that project configuration.
     * This identifier is a hook that the database can use to figure out which
     * actual, full project is being referred to. There will be many identifiers
     * possible for a given project config:
     * 1) A source could change
     * 2) A source could be present on one run and absent on another
     * @return the wres-generated project identifier
     */

    String getProjectIdentifier()
    {
        if ( projectIdentifier == null )
        {
            throw new UnsupportedOperationException( "Might not be implemented" );
        }
        return this.projectIdentifier;
    }


    /**
     * The graphics module needs to get its specific xml snippets from the
     * project config.
     * @return a Map from a DestinationConfig to a String
     */

    Map<DestinationConfig, String> getGraphicsStrings()
    {
        return this.getProjectConfigPlus()
                   .getGraphicsStrings();
    }

    private Map<FeaturePlus, Map<MetricConfigName, ThresholdsByType>> getThresholds()
    {
        return thresholds;
    }

    Map<MetricConfigName, ThresholdsByType>
    getThresholdForFeature( FeaturePlus featurePlus )
    {
        return this.getThresholds().get( featurePlus );
    }

    int getThresholdCount()
    {
        Set<Threshold> thresholds = new HashSet<>();

        // Dive through the threshold hierarchy to find what we
        // are looking for. TODO: find a better way of getting this info.
        for ( Map.Entry<FeaturePlus, Map<MetricConfigName, ThresholdsByType>> outerThresholds
                : this.getThresholds().entrySet() )
        {
            for ( Map.Entry<MetricConfigName, ThresholdsByType> middleThresholds
                  : outerThresholds.getValue().entrySet() )
            {
                for ( ThresholdsByType.ThresholdType innerThresholds
                        : middleThresholds.getValue().getStoredTypes() )
                {
                    thresholds.addAll( middleThresholds.getValue()
                                                       .getThresholdsByType( innerThresholds ) );
                }
            }
        }

        LOGGER.debug( "Thresholds found: {}", thresholds );
        // There is an additional implicit "All Data" threshold, so add one.
        return thresholds.size() + 1;
    }

    int getFeatureCount()
    {
        return this.getDecomposedFeatures().size();
    }
}
