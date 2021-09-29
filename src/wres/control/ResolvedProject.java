package wres.control;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.engine.statistics.metric.config.MetricConfigHelper;

/**
 * <p>Represents a project that has been "resolved", i.e. any kind of translation
 * from the ProjectConfig into WRES that has happened can be captured here.
 * The existence of this class is a possible solution to these discomforts:</p>
 * <ol>
 * <li>decomposed features which are disconnected from the original config</li>
 * <li>project identity in the database (discovered post-ingest)</li>
 * </ol>
 * <p>Both the decomposed features and project identity are resolved outside of
 * the original project config, and need a home for sharing. This class might
 * be that home.</p>
 *
 */
class ResolvedProject
{
    private final ProjectConfigPlus projectConfigPlus;
    private final Set<FeatureTuple> decomposedFeatures;
    private final String projectIdentifier;
    private final List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature;
    private final Path outputDirectory;

    private ResolvedProject( ProjectConfigPlus projectConfigPlus,
                             Set<FeatureTuple> decomposedFeatures,
                             String projectIdentifier,
                             List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature,
                             Path outputDirectory )
    {
        this.projectConfigPlus = projectConfigPlus;
        this.decomposedFeatures = Collections.unmodifiableSet( decomposedFeatures );
        this.projectIdentifier = projectIdentifier;
        this.thresholdsByMetricAndFeature = Collections.unmodifiableList( thresholdsByMetricAndFeature );
        this.outputDirectory = outputDirectory;
    }

    static ResolvedProject of( ProjectConfigPlus projectConfigPlus,
                               Set<FeatureTuple> decomposedFeatures,
                               String projectIdentifier,
                               List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature,
                               Path outputDirectory )
    {
        return new ResolvedProject( projectConfigPlus,
                                    decomposedFeatures,
                                    projectIdentifier,
                                    thresholdsByMetricAndFeature,
                                    outputDirectory );

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

    Set<FeatureTuple> getDecomposedFeatures()
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
    
    /**
     * @return the thresholds and metrics by feature.
     */

    List<ThresholdsByMetricAndFeature> getThresholdsByMetricAndFeature()
    {
        return this.thresholdsByMetricAndFeature;
    }

    /**
     * @return set of double score metrics used by this project
     */
    Set<MetricConstants> getDoubleScoreMetrics()
    {
        Set<MetricConstants> result = new HashSet<>();
        Set<MetricConstants> doubleScoreMetricOutputs = MetricConstants.getMetrics(
                MetricConstants.StatisticType.DOUBLE_SCORE );

        Set<MetricConstants> allMetrics = MetricConfigHelper.getMetricsFromConfig( this.getProjectConfig() );

        for ( MetricConstants doubleScoreMetric : doubleScoreMetricOutputs )
        {
            if ( allMetrics.contains( doubleScoreMetric ) )
            {
                result.add( doubleScoreMetric );
            }
        }

        return Collections.unmodifiableSet( result );
    }
    
    /**
     * @return the shared output directory to store output files
     */
    Path getOutputDirectory()
    {
        return this.outputDirectory;
    }
}
