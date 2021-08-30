package wres.io.thresholds;

import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.metrics.MetricConstants;
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
 * Maintains a thread safe map between feature tuples and the builders that will supply the metrics, with
 * their thresholds, that will need to be evaluated.
 *
 * Usage:
 * 1. Create the collection
 * 2. Initialize it with the expected set of features
 * 3. Add thresholds and metrics for features as encountered
 * 4. Call 'build' to get the final mapping between features and every one of their metrics/thresholds to evaluate
 */
public class ThresholdBuilderCollection {
    private static final Object BUILDER_ACCESS_LOCK = new Object();

    private final Map<FeatureTuple, Builder> builders = new ConcurrentHashMap<>();

    public void initialize( Set<FeatureTuple> features )
    {
        for ( FeatureTuple feature : features )
        {
            if (!this.containsFeature(feature)) {
                this.builders.put(feature, new Builder());
            }
        }
    }

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

    public void addThresholdToAll(ThresholdGroup group, MetricConstants metric, ThresholdOuter threshold) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for ( FeatureTuple feature : this.builders.keySet() )
            {
                this.addThreshold(feature, group, metric, threshold);
            }
        }
    }

    public void addThresholdsToAll(ThresholdGroup group, MetricConstants metric, Set<ThresholdOuter> thresholds) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for ( FeatureTuple feature : this.builders.keySet() )
            {
                this.addThresholds(feature, group, metric, thresholds);
            }
        }
    }

    public void addThreshold( FeatureTuple feature, ThresholdGroup group, MetricConstants metric, ThresholdOuter threshold )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            Builder builder = this.getCorrespondingBuilder( feature );
            builder.addThreshold(group, metric, threshold);
        }
    }

    public void addThresholds( FeatureTuple feature, ThresholdGroup group, MetricConstants metric, Set<ThresholdOuter> thresholds )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            Builder builder = this.getCorrespondingBuilder(feature);
            builder.addThresholds(group, metric, thresholds);
        }
    }

    public boolean containsFeature( final FeatureTuple feature )
    {
        return this.getCorrespondingBuilder(feature) != null;
    }

    public void addAllDataThresholds( final ProjectConfig projectConfig, final MetricsConfig config )
    {
        for ( MetricConstants metricName : DataFactory.getMetricsFromMetricsConfig( config, projectConfig ) )
        {
            if ( ! ( metricName.isInGroup( MetricConstants.SampleDataGroup.DICHOTOMOUS )
                     || metricName.isInGroup( MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY ) ) )
            {
                this.addThresholdToAll(
                                        ThresholdConstants.ThresholdGroup.VALUE,
                                        metricName,
                                        ThresholdOuter.ALL_DATA );
            }
        }
    }

    public boolean isEmpty() {
        synchronized (BUILDER_ACCESS_LOCK) {
            return this.builders.values().stream().allMatch(Builder::isEmpty);
        }
    }

    public Builder getCorrespondingBuilder( final FeatureTuple feature )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            for ( FeatureTuple storedFeature : this.builders.keySet() )
            {
                if ( feature.equals( storedFeature ) )
                {
                    return this.builders.get(storedFeature);
                }
            }

            Builder newBuilder = new Builder();
            this.builders.put(feature, newBuilder);

            return newBuilder;
        }
    }

    public int featureCount() {
        return this.builders.size();
    }

}
