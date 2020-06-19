package wres.io.thresholds;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.datamodel.MetricConstants;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ThresholdBuilderCollection {
    private static final Object BUILDER_ACCESS_LOCK = new Object();

    public void initialize(Collection<Feature> features) {
        for (Feature feature : features) {
            if (!this.containsFeature(feature)) {
                this.builders.put(feature, new ThresholdsByMetricBuilder());
            }
        }
    }

    public Map<Feature, ThresholdsByMetric> build() {
        return this.builders.entrySet()
                .parallelStream()
                .collect(
                        Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                mapping -> mapping.getValue().build()
                        )
                );
    }

    public void addThresholdToAll(ThresholdGroup group, MetricConstants metric, Threshold threshold) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for (Feature feature : this.builders.keySet()) {
                this.addThreshold(feature, group, metric, threshold);
            }
        }
    }

    public void addThresholdsToAll(ThresholdGroup group, MetricConstants metric, Set<Threshold> thresholds) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for (Feature feature : this.builders.keySet()) {
                this.addThresholds(feature, group, metric, thresholds);
            }
        }
    }

    public void addThreshold(Feature feature, ThresholdGroup group, MetricConstants metric, Threshold threshold) {
        synchronized (BUILDER_ACCESS_LOCK) {
            ThresholdsByMetricBuilder builder = this.getCorrespondingBuilder(feature);
            builder.addThreshold(group, metric, threshold);
        }
    }

    public void addThresholds(Feature feature, ThresholdGroup group, MetricConstants metric, Set<Threshold> thresholds) {
        synchronized (BUILDER_ACCESS_LOCK) {
            ThresholdsByMetricBuilder builder = this.getCorrespondingBuilder(feature);
            builder.addThresholds(group, metric, thresholds);
        }
    }

    public boolean containsFeature(final Feature feature) {
        return this.getCorrespondingBuilder(feature) != null;
    }

    public boolean isEmpty() {
        synchronized (BUILDER_ACCESS_LOCK) {
            return this.builders.values().stream().allMatch(ThresholdsByMetricBuilder::isEmpty);
        }
    }

    public ThresholdsByMetricBuilder getCorrespondingBuilder(final Feature feature) {
        synchronized (BUILDER_ACCESS_LOCK) {
            for (Feature storedFeature : this.builders.keySet()) {
                String featureLID = feature.getLocationId();
                String featureGage = feature.getGageId();

                String storedFeatureLID = storedFeature.getLocationId();
                String storedFeatureGage = storedFeature.getGageId();

                boolean lidsMatch = featureLID != null && featureLID.equals(storedFeatureLID);
                boolean gagesMatch = featureGage != null && featureGage.equals(storedFeatureGage);

                if (lidsMatch || gagesMatch) {
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

    private final Map<Feature, ThresholdsByMetricBuilder> builders = new ConcurrentHashMap<>();
}
