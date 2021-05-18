package wres.datamodel.metrics;

import java.util.Objects;
import java.util.Set;

import net.jcip.annotations.Immutable;
import wres.config.generated.MetricsConfig;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsByMetric;

/**
 * The {@link Metrics} represents the {@link MetricsConfig} with abstractions that support behavior, such as the 
 * determination of metric type from {@link MetricConstants} and the filtering or transformation of pairs with 
 * {@link ThresholdOuter}.
 * 
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class Metrics
{

    /**
     * Thresholds by metric.
     */

    private final ThresholdsByMetric thresholdsByMetric;
    
    /**
     * The minimum sample size.
     */

    private int minimumSampleSize;
    
    /**
     * Returns an instance from the inputs.
     * 
     * @param thresholdsByMetric the thresholds mapped to metrics
     * @param minimumSampleSize the minimum sample size
     * @throws IllegalArgumentException if the minimum sample size is less than zero
     * @throws NullPointerException if the thresholdsByMetric is null
     */
    
    public static Metrics of( ThresholdsByMetric thresholdsByMetric, int minimumSampleSize )
    {
        return new Metrics( thresholdsByMetric, minimumSampleSize );
    }
    
    /**
     * @return the thresholds mapped to metrics.
     */

    public ThresholdsByMetric getThresholdsByMetric()
    {
        return thresholdsByMetric;
    }
    
    /**
     * @return the metrics.
     */

    public Set<MetricConstants> getMetrics()
    {
        ThresholdsByMetric innerThresholdsByMetric = this.getThresholdsByMetric();
        
        return innerThresholdsByMetric.getMetrics();
    }

    /**
     * @return the minimum sample size.
     */

    public int getMinimumSampleSize()
    {
        return this.minimumSampleSize;
    }

    /**
     * Hidden constructor.
     * 
     * @param thresholdsByMetric the thresholds mapped to metrics
     * @param minimumSampleSize the minimum sample size
     * @throws IllegalArgumentException if the minimum sample size is less than zero
     * @throws NullPointerException if the thresholdsByMetric is null
     */

    private Metrics( ThresholdsByMetric thresholdsByMetric, int minimumSampleSize )
    {
        Objects.requireNonNull( thresholdsByMetric );
        
        this.thresholdsByMetric = thresholdsByMetric;
        this.minimumSampleSize = minimumSampleSize;

        if ( this.minimumSampleSize < 0 )
        {
            throw new IllegalArgumentException( "The minimum sample size must be greater than zero but was "
                                                + this.minimumSampleSize
                                                + "." );
        }
    }

}
