package wres.datamodel.thresholds;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.Immutable;
import wres.config.generated.EnsembleAverageType;
import wres.config.generated.MetricsConfig;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;

/**
 * There is one {@link ThresholdsByMetricAndFeature} for each {@link MetricsConfig}. Abstractions within this class 
 * support behavior, such as the determination of metric type from {@link MetricConstants} and the filtering or 
 * transformation of pairs with {@link ThresholdOuter}.
 * 
 * @author James Brown
 */

@Immutable
public class ThresholdsByMetricAndFeature
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdsByMetricAndFeature.class );

    /** Thresholds by metric and feature. */
    private final Map<FeatureTuple, ThresholdsByMetric> thresholds;

    /** Union of all metrics. */
    private final Set<MetricConstants> metrics;

    /** The minimum sample size. */
    private final int minimumSampleSize;

    /** The type of ensemble average to use. */
    private final EnsembleAverageType ensembleAverageType;

    /**
     * Returns an instance from the inputs.
     * 
     * @param thresholds the thresholds mapped to metrics and features
     * @param minimumSampleSize the minimum sample size
     * @return an instance
     * @throws IllegalArgumentException if the minimum sample size is less than zero
     * @throws NullPointerException if the thresholdsByMetric is null
     */

    public static ThresholdsByMetricAndFeature of( Map<FeatureTuple, ThresholdsByMetric> thresholds,
                                                   int minimumSampleSize )
    {
        return new ThresholdsByMetricAndFeature( thresholds, minimumSampleSize, null );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param thresholds the thresholds mapped to metrics and features
     * @param minimumSampleSize the minimum sample size
     * @param ensembleAverageType the type of averaging to use when translating an ensemble to a single value, optional
     * @return an instance
     * @throws IllegalArgumentException if the minimum sample size is less than zero
     * @throws NullPointerException if the thresholdsByMetric is null
     */

    public static ThresholdsByMetricAndFeature of( Map<FeatureTuple, ThresholdsByMetric> thresholds,
                                                   int minimumSampleSize,
                                                   EnsembleAverageType ensembleAverageType )
    {
        return new ThresholdsByMetricAndFeature( thresholds, minimumSampleSize, ensembleAverageType );
    }

    /**
     * @return the thresholds mapped to metrics and features.
     */

    public Map<FeatureTuple, ThresholdsByMetric> getThresholdsByMetricAndFeature()
    {
        return this.thresholds; // Immutable on construction
    }

    /**
     * @param featureGroup the feature group
     * @return an independent instance containing all features within the declared group
     * @throws NullPointerException if the featureGroup is null
     */

    public ThresholdsByMetricAndFeature getThresholdsByMetricAndFeature( FeatureGroup featureGroup )
    {
        Objects.requireNonNull( featureGroup );

        Set<FeatureTuple> features = featureGroup.getFeatures();

        Map<FeatureTuple, ThresholdsByMetric> innerThresholds =
                this.thresholds.entrySet()
                               .stream()
                               .filter( nextEntry -> features.contains( nextEntry.getKey() ) )
                               .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                                       Map.Entry::getValue ) );

        return new ThresholdsByMetricAndFeature( innerThresholds,
                                                 this.getMinimumSampleSize(),
                                                 this.getEnsembleAverageType() );
    }

    /**
     * @return the metrics.
     */

    public Set<MetricConstants> getMetrics()
    {
        return this.metrics;
    }

    /**
     * @return the minimum sample size.
     */

    public int getMinimumSampleSize()
    {
        return this.minimumSampleSize;
    }

    /**
     * @return the type of ensemble averaging to use.
     */

    public EnsembleAverageType getEnsembleAverageType()
    {
        return this.ensembleAverageType;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof ThresholdsByMetricAndFeature ) )
        {
            return false;
        }

        if ( o == this )
        {
            return true;
        }

        ThresholdsByMetricAndFeature in = (ThresholdsByMetricAndFeature) o;

        return Objects.equals( this.getThresholdsByMetricAndFeature(), in.getThresholdsByMetricAndFeature() )
               && this.getMinimumSampleSize() == in.getMinimumSampleSize();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getThresholdsByMetricAndFeature(), this.getMinimumSampleSize() );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "thresholdsByMetricAndFeature",
                                                                                     this.getThresholdsByMetricAndFeature() )
                                                                            .append( "minimumSampleSize",
                                                                                     this.getMinimumSampleSize() )
                                                                            .toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param thresholds the thresholds mapped to metrics and features
     * @param minimumSampleSize the minimum sample size
     * @param ensembleAverageType the type of averaging to use when translating an ensemble to a single value, optional
     * @throws IllegalArgumentException if the minimum sample size is less than zero
     * @throws NullPointerException if the thresholdsByMetric is null
     */

    private ThresholdsByMetricAndFeature( Map<FeatureTuple, ThresholdsByMetric> thresholds,
                                          int minimumSampleSize,
                                          EnsembleAverageType ensembleAverageType )
    {
        Objects.requireNonNull( thresholds );

        if ( minimumSampleSize < 0 )
        {
            throw new IllegalArgumentException( "The minimum sample size must be greater than zero but was "
                                                + minimumSampleSize
                                                + "." );
        }

        // Default to ensemble mean where applicable
        if ( Objects.isNull( ensembleAverageType ) )
        {
            LOGGER.debug( "Using a default ensemble average that corresponds to the {}.", EnsembleAverageType.MEAN );

            this.ensembleAverageType = EnsembleAverageType.MEAN;
        }
        else
        {
            LOGGER.debug( "Setting the ensemble average type to {}.", ensembleAverageType);
            
            this.ensembleAverageType = ensembleAverageType;
        }

        this.thresholds =
                Collections.unmodifiableMap( new HashMap<>( thresholds ) );
        this.minimumSampleSize = minimumSampleSize;
        this.metrics = this.getThresholdsByMetricAndFeature()
                           .values()
                           .stream()
                           .flatMap( next -> next.getMetrics().stream() )
                           .collect( Collectors.toUnmodifiableSet() );
    }

}
