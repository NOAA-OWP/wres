package wres.io.thresholds;

import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ThresholdBuilderCollection {
    private static final Object BUILDER_ACCESS_LOCK = new Object();

    private final Map<String, ThresholdsByMetricBuilder> builders = new ConcurrentHashMap<>();

    public void initialize( Set<String> features )
    {
        for ( String feature : features )
        {
            if (!this.containsFeature(feature)) {
                this.builders.put(feature, new ThresholdsByMetricBuilder());
            }
        }
    }

    public Map<String, ThresholdsByMetric> build()
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
            for ( String feature : this.builders.keySet() )
            {
                this.addThreshold(feature, group, metric, threshold);
            }
        }
    }

    public void addThresholdsToAll(ThresholdGroup group, MetricConstants metric, Set<ThresholdOuter> thresholds) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for ( String feature : this.builders.keySet() )
            {
                this.addThresholds(feature, group, metric, thresholds);
            }
        }
    }

    public void addThreshold( String feature, ThresholdGroup group, MetricConstants metric, ThresholdOuter threshold )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            ThresholdsByMetricBuilder builder = this.getCorrespondingBuilder( feature );
            builder.addThreshold(group, metric, threshold);
        }
    }

    public void addThresholds( String feature, ThresholdGroup group, MetricConstants metric, Set<ThresholdOuter> thresholds )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            ThresholdsByMetricBuilder builder = this.getCorrespondingBuilder(feature);
            builder.addThresholds(group, metric, thresholds);
        }
    }

    public boolean containsFeature( final String feature )
    {
        return this.getCorrespondingBuilder(feature) != null;
    }

    public void addAllDataThresholds( final ProjectConfig projectConfig )
    {
        for ( MetricsConfig config: projectConfig.getMetrics() )
        {
            for (MetricConstants metricName : DataFactory.getMetricsFromMetricsConfig(config, projectConfig)) {
                if (
                        !(
                                metricName.isInGroup(MetricConstants.SampleDataGroup.DICHOTOMOUS)
                                        || metricName.isInGroup(MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY)
                        )
                ) {
                    this.addThresholdToAll(
                            ThresholdConstants.ThresholdGroup.VALUE,
                            metricName,
                            ThresholdOuter.ALL_DATA
                    );
                }
            }
        }
    }

    public boolean isEmpty() {
        synchronized (BUILDER_ACCESS_LOCK) {
            return this.builders.values().stream().allMatch(ThresholdsByMetricBuilder::isEmpty);
        }
    }

    public ThresholdsByMetricBuilder getCorrespondingBuilder( final String feature )
    {
        synchronized (BUILDER_ACCESS_LOCK) {
            for ( String storedFeature : this.builders.keySet() )
            {
                if ( feature.equals( storedFeature ) )
                {
                    return this.builders.get(storedFeature);
                }
            }

            ThresholdsByMetricBuilder newBuilder = new ThresholdsByMetricBuilder();
            this.builders.put(feature, newBuilder);

            return newBuilder;
        }
    }

    public int featureCount() {
        return this.builders.size();
    }

}
