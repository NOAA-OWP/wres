package wres.io.thresholds;

import wres.config.generated.*;
import wres.config.MetricConstants;
import wres.config.xml.MetricConstantsFactory;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsGenerator;

import java.util.*;

/**
 * Reads thresholds from the project declaration.
 */
public class InBandThresholdReader
{
    private final ProjectConfig projectConfig;
    private final MetricsConfig metricsConfig;
    private final MeasurementUnit measurementUnits;

    /**
     * Creates an instance.
     * @param projectConfig the project declaration
     * @param metricsConfig the metrics declaration
     * @param measurementUnits the measurement units
     */
    public InBandThresholdReader( ProjectConfig projectConfig,
                                  MetricsConfig metricsConfig,
                                  MeasurementUnit measurementUnits )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( metricsConfig );
        Objects.requireNonNull( measurementUnits );

        this.projectConfig = projectConfig;
        this.metricsConfig = metricsConfig;
        this.measurementUnits = measurementUnits;
    }

    /**
     * Read the thresholds.
     * @return the thresholds
     */
    public Set<ThresholdOuter> read()
    {
        Set<ThresholdOuter> finalThresholds = new HashSet<>();
        for ( ThresholdsConfig thresholdsConfig : this.getThresholds( this.metricsConfig ) )
        {
            Set<ThresholdOuter> nextThresholds = this.readThresholds(
                    thresholdsConfig,
                    MetricConstantsFactory.getMetricsFromConfig( this.metricsConfig, this.projectConfig ) );
            finalThresholds.addAll( nextThresholds );
        }

        // Add an "all data" threshold if there was no threshold declaration (for which an "all data" threshold is
        // added as needed)
        if( finalThresholds.isEmpty() )
        {
            finalThresholds.add( ThresholdOuter.ALL_DATA );
        }

        return Collections.unmodifiableSet( finalThresholds );
    }

    /**
     * @param metrics the metrics declaration
     * @return the thresholds
     */
    private Set<ThresholdsConfig> getThresholds( MetricsConfig metrics )
    {
        Set<ThresholdsConfig> thresholdsWithoutSources = new HashSet<>();

        for ( ThresholdsConfig thresholdsConfig : metrics.getThresholds() )
        {
            if ( !( thresholdsConfig.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source ) )
            {
                thresholdsWithoutSources.add( thresholdsConfig );
            }
        }

        return thresholdsWithoutSources;
    }

    /**
     * Reads the thresholds.
     * @param thresholdsConfig the thresholds declaration
     * @param metrics the metrics
     * @return the thresholds
     */
    private Set<ThresholdOuter> readThresholds( ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( projectConfig, "Specify a non-null project configuration." );

        // Thresholds
        Set<ThresholdOuter> thresholds = ThresholdsGenerator.getThresholdsFromThresholdsConfig( thresholdsConfig,
                                                                                                this.measurementUnits );

        Set<ThresholdOuter> finalThresholds = new HashSet<>();

        for ( MetricConstants metric : metrics )
        {
            // Type of thresholds
            wres.config.yaml.components.ThresholdType thresholdType =
                    wres.config.yaml.components.ThresholdType.PROBABILITY;
            if ( Objects.nonNull( thresholdsConfig.getType() ) )
            {
                thresholdType = ThresholdsGenerator.getThresholdGroup( thresholdsConfig.getType() );
            }

            Set<ThresholdOuter> adjustedThresholds = ThresholdsGenerator.getAdjustedThresholds( metric,
                                                                                                thresholds,
                                                                                                thresholdType );
            finalThresholds.addAll( adjustedThresholds );

            // Add an all data threshold
            if( metric.isContinuous() )
            {
                finalThresholds.add( ThresholdOuter.ALL_DATA );
            }
        }

        return Collections.unmodifiableSet( finalThresholds );
    }
}
