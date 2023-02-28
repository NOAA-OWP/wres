package wres.io.thresholds;

import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstantsFactory;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.Builder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * <p>Maintains a thread safe map between feature tuples and the builders that will supply the metrics, with
 * their thresholds, that will need to be evaluated.
 *
 * <p>Usage:
 * 1. Create the collection
 * 2. Initialize it with the expected set of features
 * 3. Add thresholds and metrics for features as encountered
 * 4. Call 'build' to get the final mapping between features and every one of their metrics/thresholds to evaluate
 */
public class ThresholdBuilderCollection
{
    private static final Object BUILDER_ACCESS_LOCK = new Object();
    private final Map<FeatureTuple, Builder> builders = new ConcurrentHashMap<>();

    /**
     * Initializes the builders
     * @param features the features
     */
    public void initialize( Set<FeatureTuple> features )
    {
        for ( FeatureTuple feature : features )
        {
            if ( !this.containsFeature( feature ) )
            {
                this.builders.put( feature, new Builder() );
            }
        }
    }

    /**
     * @return an instance
     */
    public Map<FeatureTuple, ThresholdsByMetric> build()
    {
        return this.builders.entrySet()
                            .parallelStream()
                            .collect(
                                    Collectors.toUnmodifiableMap(
                                            Map.Entry::getKey,
                                            mapping -> mapping.getValue().build()
                                    )
                            );
    }

    /**
     * Adds a threshold to all features.
     * @param group the threshold group
     * @param metric the metric
     * @param threshold the threshold
     */
    public void addThresholdToAll( ThresholdGroup group, MetricConstants metric, ThresholdOuter threshold )
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            for ( FeatureTuple feature : this.builders.keySet() )
            {
                this.addThreshold( feature, group, metric, threshold );
            }
        }
    }

    /**
     * Adds thresholds to all features.
     * @param group the threshold group
     * @param metric the metric
     * @param thresholds the thresholds
     */
    public void addThresholdsToAll( ThresholdGroup group, MetricConstants metric, Set<ThresholdOuter> thresholds )
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            for ( FeatureTuple feature : this.builders.keySet() )
            {
                this.addThresholds( feature, group, metric, thresholds );
            }
        }
    }

    /**
     * Adds a threshold.
     * @param feature the feature
     * @param group the threshold group
     * @param metric the metric
     * @param threshold the threshold
     */
    public void addThreshold( FeatureTuple feature,
                              ThresholdGroup group,
                              MetricConstants metric,
                              ThresholdOuter threshold )
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            Builder builder = this.getCorrespondingBuilder( feature );
            builder.addThreshold( group, metric, threshold );
        }
    }

    /**
     * Adds thresholds.
     * @param feature the feature
     * @param group the threshold group
     * @param metric the metrics
     * @param thresholds the thresholds
     */
    public void addThresholds( FeatureTuple feature,
                               ThresholdGroup group,
                               MetricConstants metric,
                               Set<ThresholdOuter> thresholds )
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            Builder builder = this.getCorrespondingBuilder( feature );
            builder.addThresholds( group, metric, thresholds );
        }
    }

    /**
     * @param feature the feature
     * @return whether thresholds exist for the prescribed feature
     */
    public boolean containsFeature( final FeatureTuple feature )
    {
        return this.getCorrespondingBuilder( feature ) != null;
    }

    /**
     * Adds the all data thresholds.
     * @param projectConfig the project declaration
     * @param metricsConfig the metrics declaration
     */
    public void addAllDataThresholds( final ProjectConfig projectConfig, final MetricsConfig metricsConfig )
    {
        for ( MetricConstants metricName : MetricConstantsFactory.getMetricsFromConfig( metricsConfig, projectConfig ) )
        {
            if ( !( metricName.isInGroup( MetricConstants.SampleDataGroup.DICHOTOMOUS )
                    || metricName.isInGroup( MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY ) ) )
            {
                this.addThresholdToAll(
                        ThresholdConstants.ThresholdGroup.VALUE,
                        metricName,
                        ThresholdOuter.ALL_DATA );
            }
        }
    }

    /**
     * @return whether the builders are empty
     */
    public boolean isEmpty()
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            return this.builders.values().stream().allMatch( Builder::isEmpty );
        }
    }

    /**
     * @param feature the feature
     * @return the builder
     */
    public Builder getCorrespondingBuilder( final FeatureTuple feature )
    {
        synchronized ( BUILDER_ACCESS_LOCK )
        {
            for ( Map.Entry<FeatureTuple, Builder> nextEntry : this.builders.entrySet() )
            {
                FeatureTuple storedFeature = nextEntry.getKey();
                Builder builder = nextEntry.getValue();
                if ( feature.equals( storedFeature ) )
                {
                    return builder;
                }
            }

            Builder newBuilder = new Builder();
            this.builders.put( feature, newBuilder );

            return newBuilder;
        }
    }
}
