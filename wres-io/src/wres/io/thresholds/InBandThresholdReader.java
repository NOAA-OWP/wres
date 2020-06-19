package wres.io.thresholds;

import wres.config.MetricConfigException;
import wres.config.generated.*;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;

import java.util.*;
import java.util.stream.Collectors;

public class InBandThresholdReader {
    public InBandThresholdReader(final ProjectConfig projectConfig, final ThresholdBuilderCollection sharedBuilders) {
        this.sharedBuilders = sharedBuilders;
        this.projectConfig = projectConfig;
    }

    public void read() {
        for (MetricsConfig config : this.projectConfig.getMetrics()) {
            for (ThresholdsConfig thresholdsConfig : this.getThresholds(config)) {
                this.readThresholds(
                        thresholdsConfig,
                        DataFactory.getMetricsFromMetricsConfig(config, this.projectConfig)
                );
            }
        }

        this.addDefaultThresholds();
    }

    private void addDefaultThresholds() {
        Threshold allData = Threshold.of(
                OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                ThresholdConstants.Operator.GREATER,
                ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT
        );

        for (MetricsConfig config: this.projectConfig.getMetrics()) {
            if (config.getThresholds().isEmpty()) {
                for (MetricConstants metricName : DataFactory.getMetricsFromMetricsConfig(config, this.projectConfig)) {
                    this.sharedBuilders.addThresholdToAll(
                            ThresholdConstants.ThresholdGroup.VALUE,
                            metricName,
                            allData
                    );
                }
            }
        }
    }

    private Set<ThresholdsConfig> getThresholds(MetricsConfig metrics) {
        Set<ThresholdsConfig> thresholdsWithoutSources = new HashSet<>();

        for (ThresholdsConfig thresholdsConfig : metrics.getThresholds()) {
            if (!(thresholdsConfig.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source)) {
                thresholdsWithoutSources.add(thresholdsConfig);
            }
        }

        return thresholdsWithoutSources;
    }

    /**
     * Returns an adjusted set of thresholds for the input thresholds. Adjustments are made for the specific metric.
     * Notably, an "all data" threshold is added for metrics that support this, and thresholds are removed for metrics
     * that do not support them.
     *
     * @param metric the metric
     * @param thresholds the raw thresholds
     * @param group the threshold type
     * @return the adjusted thresholds
     * @throws NullPointerException if either input is null
     */

    private static Set<Threshold> includeAllDataThreshold(MetricConstants metric,
                                                          Set<Threshold> thresholds,
                                                          ThresholdConstants.ThresholdGroup group )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric." );

        Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

        Objects.requireNonNull( group, "Specify a non-null threshold type." );

        if ( group == ThresholdConstants.ThresholdGroup.PROBABILITY_CLASSIFIER )
        {
            return thresholds;
        }

        Threshold allData = Threshold.of(
                OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                ThresholdConstants.Operator.GREATER,
                ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT
        );

        // All data only
        if ( metric.getMetricOutputGroup() == MetricConstants.StatisticType.BOXPLOT_PER_PAIR
                || metric.getMetricOutputGroup() == MetricConstants.StatisticType.BOXPLOT_PER_POOL
                || metric == MetricConstants.QUANTILE_QUANTILE_DIAGRAM )
        {
            return Set.copyOf(Collections.singletonList(allData));
        }

        // Otherwise, add the input thresholds
        Set<Threshold> combinedThresholds = new HashSet<>(thresholds);

        // Add all data for appropriate types
        if ( metric.isInGroup( MetricConstants.SampleDataGroup.ENSEMBLE )
                || metric.isInGroup( MetricConstants.SampleDataGroup.SINGLE_VALUED )
                || metric.isInGroup( MetricConstants.SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            combinedThresholds.add( allData );
        }

        return Collections.unmodifiableSet( combinedThresholds );
    }

    private void readThresholds(ThresholdsConfig thresholdsConfig, Set<MetricConstants> metrics) {
        Objects.requireNonNull( projectConfig, "Specify a non-null project configuration." );

        // Find the units associated with the pairs
        MeasurementUnit units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = MeasurementUnit.of(projectConfig.getPair().getUnit() );
        }

        // Thresholds
        Set<Threshold> thresholds = InBandThresholdReader.createThresholds( thresholdsConfig, units );

        for (MetricConstants metric : metrics) {

            // Type of thresholds
            ThresholdConstants.ThresholdGroup thresholdGroup = ThresholdConstants.ThresholdGroup.PROBABILITY;
            if ( Objects.nonNull( thresholdsConfig.getType() ) )
            {
                thresholdGroup = DataFactory.getThresholdGroup( thresholdsConfig.getType() );
            }

            Set<Threshold> adjustedThresholds = InBandThresholdReader.includeAllDataThreshold(metric, thresholds, thresholdGroup);

            this.sharedBuilders.addThresholdsToAll(thresholdGroup, metric, adjustedThresholds);
        }
    }

    /**
     * Obtains a set of {@link Threshold} from a {@link ThresholdsConfig} for thresholds that are configured internally
     * and not sourced externally. In other words, {@link ThresholdsConfig#getCommaSeparatedValuesOrSource()} must
     * return a string of thresholds and not a {@link ThresholdsConfig.Source}.
     *
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     */

    private static Set<Threshold> createThresholds(ThresholdsConfig thresholds, MeasurementUnit units )
    {
        Objects.requireNonNull( thresholds, "Specify non-null thresholds configuration." );

        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;

        // Operator specified
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( thresholds );
        }

        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;

        // Operator specified
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            dataType = DataFactory.getThresholdDataType( thresholds.getApplyTo() );
        }

        // Must be internally sourced: thresholds with global scope should be provided directly
        Object values = thresholds.getCommaSeparatedValuesOrSource();

        // Default to ThresholdType.PROBABILITY
        ThresholdType type = ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            type = thresholds.getType();
        }

        // String = internal source
        if ( values instanceof String )
        {
            return InBandThresholdReader.readCommaSeparatedValues(
                    values.toString(),
                    operator,
                    dataType,
                    type != ThresholdType.VALUE,
                    units
            );
        }

        return Set.of();
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     *
     * @param inputString the comma-separated input string
     * @param operator the operator
     * @param dataType the data type to which the threshold applies
     * @param valuesAreProbabilistic is true to generate probability thresholds, false for ordinary thresholds
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Set<Threshold> readCommaSeparatedValues(
            String inputString,
            ThresholdConstants.Operator operator,
            ThresholdConstants.ThresholdDataType dataType,
            boolean valuesAreProbabilistic,
            MeasurementUnit units
    )
    {
        Objects.requireNonNull( inputString, "Specify a non-null input string." );

        Objects.requireNonNull( operator, "Specify a non-null operator." );

        Objects.requireNonNull( dataType, "Specify a non-null data type." );

        //Parse the double values
        List<Double> valuesToAdd = Arrays.stream( inputString.split( "," ) )
                .map( Double::parseDouble )
                .collect( Collectors.toList() );

        Set<Threshold> commaSeparatedThresholds = new TreeSet<>();

        //Between operator
        if ( operator == ThresholdConstants.Operator.BETWEEN )
        {
            return InBandThresholdReader.readBetweenThresholds(
                    valuesToAdd,
                    dataType,
                    valuesAreProbabilistic,
                    units
            );
        }
        //Other operators
        else
        {
            if ( valuesAreProbabilistic )
            {
                valuesToAdd.forEach(
                        value -> commaSeparatedThresholds.add(
                                Threshold.ofProbabilityThreshold(
                                        OneOrTwoDoubles.of( value ),
                                        operator,
                                        dataType,
                                        units
                                )
                        )
                );
            }
            else
            {
                valuesToAdd.forEach(
                        value -> commaSeparatedThresholds.add(
                                Threshold.of(
                                        OneOrTwoDoubles.of( value ),
                                        operator,
                                        dataType,
                                        units
                                )
                        )
                );
            }
        }

        return Collections.unmodifiableSet( commaSeparatedThresholds );
    }

    public static Set<Threshold> readBetweenThresholds(
            List<Double> valuesToAdd,
            ThresholdConstants.ThresholdDataType dataType,
            boolean valuesAreProbabilistic,
            MeasurementUnit units
    ) {
        Set<Threshold> thresholdsBetween = new HashSet<>();
        if ( valuesToAdd.size() < 2 )
        {
            throw new MetricConfigException( "At least two values are required to compose a "
                    + "threshold that operates between a lower and an upper bound." );
        }
        for ( int i = 0; i < valuesToAdd.size() - 1; i++ )
        {
            Threshold newThreshold;

            if ( valuesAreProbabilistic )
            {
                newThreshold = Threshold.ofProbabilityThreshold(
                        OneOrTwoDoubles.of( valuesToAdd.get( i ), valuesToAdd.get( i + 1 ) ),
                        ThresholdConstants.Operator.BETWEEN,
                        dataType,
                        units
                );
            }
            else
            {
                newThreshold = Threshold.of(
                        OneOrTwoDoubles.of( valuesToAdd.get( i ), valuesToAdd.get( i + 1 ) ),
                        ThresholdConstants.Operator.BETWEEN,
                        dataType,
                        units
                );
            }

            thresholdsBetween.add(newThreshold);
        }

        return Collections.unmodifiableSet(thresholdsBetween);
    }

    private final ProjectConfig projectConfig;
    private final ThresholdBuilderCollection sharedBuilders;
}
