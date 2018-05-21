package wres.control;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import wres.config.FeaturePlus;
import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.config.MetricConfigHelper;

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
    
    private final ProjectConfigPlus projectConfigPlus;
    private final Set<FeaturePlus> decomposedFeatures;
    private final String projectIdentifier;
    private final Map<FeaturePlus, ThresholdsByMetric>
            externalThresholds;

    private ResolvedProject( ProjectConfigPlus projectConfigPlus,
                             Set<FeaturePlus> decomposedFeatures,
                             String projectIdentifier,
                             Map<FeaturePlus, ThresholdsByMetric> thresholds )
    {
        this.projectConfigPlus = projectConfigPlus;
        this.decomposedFeatures = Collections.unmodifiableSet( decomposedFeatures );
        this.projectIdentifier = projectIdentifier;
        this.externalThresholds = Collections.unmodifiableMap( thresholds );
    }

    static ResolvedProject of( ProjectConfigPlus projectConfigPlus,
                               Set<FeaturePlus> decomposedFeatures,
                               String projectIdentifier,
                               Map<FeaturePlus, ThresholdsByMetric> thresholds )
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

    private Map<FeaturePlus, ThresholdsByMetric> getExternalThresholds()
    {
        return this.externalThresholds;
    }

    ThresholdsByMetric getThresholdForFeature( FeaturePlus featurePlus )
    {
        return this.getExternalThresholds().get( featurePlus );
    }
    
    /**
     * Returns the cardinality of the set of thresholds that apply across 
     * all features. These include thresholds that are sourced externally 
     * and apply to specific features and thresholds that are sourced
     * from within the project configuration and apply to all features. 
     * The cardinality refers to the set of composed thresholds used
     * to produce the metric output and not the total number of 
     * thresholds, i.e. there is one composed threshold for each output.  
     * 
     * @param outGroup an optional output group for which the 
     *            cardinality is required, may be null for all groups
     * @return the cardinality of the set of thresholds
     * @throws MetricConfigException if the configuration of 
     *            thresholds is incorrect
     */
    
    int getThresholdCount( MetricOutputGroup outGroup )
    {
        // Obtain the union of internal and external thresholds
        DataFactory thresholdFactory = DefaultDataFactory.getInstance();
        ThresholdsByMetric thresholds =
                MetricConfigHelper.getThresholdsFromConfig( this.getProjectConfig(),
                                                            thresholdFactory,
                                                            externalThresholds.values() );
        // Filter the thresholds if required
        if( Objects.nonNull( outGroup ) )
        {
            thresholds = thresholds.filterByGroup( outGroup );
        }
        
        // Return the cardinality of the set of composed thresholds
        return thresholds.unionOfOneOrTwoThresholds().size();
    }

    int getFeatureCount()
    {
        return this.getDecomposedFeatures().size();
    }

    /**
     * @return set of double score metrics used by this project
     */
    Set<MetricConstants> getDoubleScoreMetrics()
    {
        Set<MetricConstants> result = new HashSet<>();
        Set<MetricConstants> doubleScoreMetricOutputs = MetricConstants.getMetrics(
                MetricConstants.MetricOutputGroup.DOUBLE_SCORE );

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
}
