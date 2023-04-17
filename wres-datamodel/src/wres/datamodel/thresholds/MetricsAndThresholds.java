package wres.datamodel.thresholds;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import wres.config.MetricConstants;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Pool;

/**
 * A small value class that represents an atomic collection of metrics for processing. In other words, every metric in
 * this class has a common set of thresholds and other parameters.
 *
 * @param metrics the metrics
 * @param thresholds the thresholds
 * @param minimumSampleSize the minimum sample size
 * @param ensembleAverageType the ensemble averaging method, where applicable
 * @author James Brown
 */
public record MetricsAndThresholds( Set<MetricConstants> metrics,
                                    Map<FeatureTuple, Set<ThresholdOuter>> thresholds,
                                    int minimumSampleSize,
                                    Pool.EnsembleAverageType ensembleAverageType )
{
    /**
     * Creates an instance, handling validation and defaults.
     * @param metrics the metrics, required
     * @param thresholds the thresholds, required
     * @param minimumSampleSize the minimum sample size
     * @param ensembleAverageType the ensemble averaging method
     */
    public MetricsAndThresholds
    {
        Objects.requireNonNull( metrics );
        Objects.requireNonNull( thresholds );

        if( Objects.isNull( ensembleAverageType ) )
        {
            ensembleAverageType = Pool.EnsembleAverageType.MEAN;
        }

        thresholds = Collections.unmodifiableMap( thresholds );
    }
}
