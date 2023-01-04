package wres.pipeline;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import wres.config.ProjectConfigPlus;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstantsFactory;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;

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
    private final String projectIdentifier;
    private final List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature;
    private final Path outputDirectory;

    private ResolvedProject( ProjectConfigPlus projectConfigPlus,
                             String projectIdentifier,
                             List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature,
                             Path outputDirectory )
    {
        this.projectConfigPlus = projectConfigPlus;
        this.projectIdentifier = projectIdentifier;
        this.thresholdsByMetricAndFeature = Collections.unmodifiableList( thresholdsByMetricAndFeature );
        this.outputDirectory = outputDirectory;
    }

    static ResolvedProject of( ProjectConfigPlus projectConfigPlus,
                               String projectIdentifier,
                               List<ThresholdsByMetricAndFeature> thresholdsByMetricAndFeature,
                               Path outputDirectory )
    {
        return new ResolvedProject( projectConfigPlus,
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

        Set<MetricConstants> allMetrics = MetricConstantsFactory.getMetricsFromConfig( this.getProjectConfig() );

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
