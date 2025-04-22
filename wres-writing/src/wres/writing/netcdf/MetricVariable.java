package wres.writing.netcdf;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.DataUtilities;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Covariate;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;

class MetricVariable
{
    private static final String LIST_DELIMITER = "; ";
    private static final String UNDEFINED = "UNDEFINED";
    private static final String UNKNOWN = "UNKNOWN";
    private static final String NONE = "NONE";
    private final ChronoUnit durationUnits;
    private final String variableName;
    private final MetricNames metricNames;
    private final String longName;
    private final String measurementUnit;
    private final String firstCondition;
    private final String firstDataType;
    private final String eventThresholdName;
    private final String secondCondition;
    private final String secondDataType;
    private final String earliestReferenceTime;
    private final String latestReferenceTime;
    private final String earliestValidTime;
    private final String latestValidTime;
    private final String timeScalePeriod;
    private final String timeScaleFunction;
    private final String timeScaleStartMonthDay;
    private final String timeScaleEndMonthDay;
    private final OneOrTwoThresholds thresholds;
    private final Number earliestLead;
    private final Number latestLead;
    private final Double sampleQuantile;
    private final Pool.EnsembleAverageType ensembleAverageType;
    private final SummaryStatistic summaryStatistic;
    private final String covariateString;

    /**
     * @return the variable name
     */

    public String getName()
    {
        return this.variableName;
    }

    /**
     * @return the metric name
     */

    public MetricNames getMetricName()
    {
        return this.metricNames;
    }

    @Override
    public String toString()
    {
        ToStringBuilder builder = new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE );

        for ( Map.Entry<String, Object> nextAttribute : this.getAttributes()
                                                            .entrySet() )
        {
            builder.append( nextAttribute.getKey(), nextAttribute.getValue() );
        }

        return builder.build();
    }

    /**
     * @return a map of attributes of the variable
     */

    Map<String, Object> getAttributes()
    {
        Map<String, Object> attributes = new TreeMap<>();

        attributes.put( "earliest_reference_time", this.earliestReferenceTime );
        attributes.put( "latest_reference_time", this.latestReferenceTime );
        attributes.put( "earliest_valid_time", this.earliestValidTime );
        attributes.put( "latest_valid_time", this.latestValidTime );

        // Add the default duration units to qualify
        attributes.put( "earliest_lead_" + this.getDurationUnits()
                                               .name()
                                               .toLowerCase(),
                        this.earliestLead );
        attributes.put( "latest_lead_" + this.getDurationUnits()
                                             .name()
                                             .toLowerCase(),
                        this.latestLead );
        attributes.put( "covariate_filters", this.covariateString );
        attributes.put( "event_threshold_data_type", this.firstDataType );
        attributes.put( "decision_threshold_data_type", this.secondDataType );
        attributes.put( "event_threshold_name", this.eventThresholdName );
        attributes.put( "measurement_unit", this.measurementUnit );
        attributes.put( "long_name", this.longName );
        attributes.put( "event_threshold_condition", this.firstCondition );
        attributes.put( "decision_threshold_condition", this.secondCondition );

        // Add the default duration units to qualify
        attributes.put( "time_scale_period_" + this.getDurationUnits()
                                                   .name()
                                                   .toLowerCase(),
                        this.timeScalePeriod );
        attributes.put( "time_scale_function", this.timeScaleFunction );
        attributes.put( "time_scale_start_month_day", this.timeScaleStartMonthDay );
        attributes.put( "time_scale_end_month_day", this.timeScaleEndMonthDay );

        attributes.put( "ensemble_average_type", this.ensembleAverageType.name() );

        if ( Objects.isNull( this.sampleQuantile ) )
        {
            attributes.put( "sample_quantile", NONE );
        }
        else
        {
            attributes.put( "sample_quantile", this.sampleQuantile );
        }

        if ( Objects.nonNull( this.summaryStatistic ) )
        {
            attributes.put( "summary_statistic_name", this.summaryStatistic.getStatistic()
                                                                           .name() );

            attributes.put( "summary_statistic_dimensions", this.summaryStatistic.getDimensionList()
                                                                                .stream()
                                                                                .map( Enum::name )
                                                                                .toList() );
            if ( this.summaryStatistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
            {
                attributes.put( "summary_statistic_quantile", this.summaryStatistic.getProbability() );
            }
            else
            {
                attributes.put( "summary_statistic_quantile", NONE );
            }
        }

        return Collections.unmodifiableMap( attributes );
    }

    /**
     * Builder.
     */
    static class Builder
    {
        private final List<Covariate> covariates = new ArrayList<>();
        private String variableName;
        private TimeWindowOuter timeWindow;
        private MetricNames metricNames;
        private OneOrTwoThresholds thresholds;
        private String units;
        private TimeScaleOuter desiredTimeScale;
        private ChronoUnit durationUnits;
        private Pool.EnsembleAverageType ensembleAverageType;
        private Double sampleQuantile;
        private SummaryStatistic summaryStatistic;

        Builder setVariableName( String variableName )
        {
            this.variableName = variableName;
            return this;
        }

        Builder setTimeWindow( TimeWindowOuter timeWindow )
        {
            this.timeWindow = timeWindow;
            return this;
        }

        Builder setMetricName( MetricNames metricNames )
        {
            this.metricNames = metricNames;
            return this;
        }

        Builder setThresholds( OneOrTwoThresholds thresholds )
        {
            this.thresholds = thresholds;
            return this;
        }

        Builder setUnits( String units )
        {
            this.units = units;
            return this;
        }

        Builder setDesiredTimeScale( TimeScaleOuter desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;
            return this;
        }

        Builder setDurationUnits( ChronoUnit durationUnits )
        {
            this.durationUnits = durationUnits;
            return this;
        }

        Builder setEnsembleAverageType( Pool.EnsembleAverageType ensembleAverageType )
        {
            this.ensembleAverageType = ensembleAverageType;
            return this;
        }

        Builder setSampleQuantile( Double sampleQuantile )
        {
            this.sampleQuantile = sampleQuantile;
            return this;
        }

        Builder setSummaryStatistic( SummaryStatistic summaryStatistic )
        {
            this.summaryStatistic = summaryStatistic;
            return this;
        }

        Builder setCovariates( List<Covariate> covariates )
        {
            if ( Objects.nonNull( covariates ) )
            {
                this.covariates.addAll( covariates );
            }
            return this;
        }

        /**
         * @return an instance
         */
        MetricVariable build()
        {
            return new MetricVariable( this );
        }
    }

    /**
     * Builds a new variable.
     *
     * @param builder the builder
     */

    private MetricVariable( Builder builder )
    {
        this.durationUnits = builder.durationUnits;
        this.thresholds = builder.thresholds;
        this.variableName = builder.variableName;
        this.ensembleAverageType = builder.ensembleAverageType;
        this.sampleQuantile = builder.sampleQuantile;
        this.metricNames = builder.metricNames;
        this.summaryStatistic = builder.summaryStatistic;

        if ( builder.covariates.isEmpty() )
        {
            this.covariateString = NONE;
        }
        else
        {
            this.covariateString = builder.covariates.stream()
                                                     .map( MessageUtilities::toString )
                                                     .collect( Collectors.joining( LIST_DELIMITER ) );
        }

        // Build the long_name. This should not contain numbers for the thresholds because they can vary by feature.
        String fullName = builder.metricNames.metricName() + " " + this.getThreshold();

        // Primary threshold has a label? If so, ignore that when replacing numbers in the long_name.
        // See #85465.
        if ( this.thresholds.first()
                            .hasLabel() )
        {
            String label = this.thresholds.first()
                                          .getLabel();
            // Replace with a temporary placeholder that contains no numbers
            fullName = fullName.replace( label, "oHhcAqApZQLTLmtDvaX" );
        }

        // Replace all numbers with a "variable number" placeholder because these numbers can vary by feature. A more 
        // sophisticated approach would determine whether they do actually vary by feature and only replace them in that 
        // case. The CF convention is silent about variable names for thresholds that vary by feature. Some related 
        // discussion here: https://is.enes.org/archive-1/phase-2/documents/Talks/2nd-joint-clipc-is-enes2-workshop-on-metadata-drs-for-climate-indices/l-barring_data-variable-attributes-for-climate-indices
        fullName = fullName.replaceAll( "\\d+", "#" );

        // Add back any label removed.
        if ( this.thresholds.first()
                            .hasLabel() )
        {
            String label = thresholds.first()
                                     .getLabel();
            fullName = fullName.replace( "oHhcAqApZQLTLmtDvaX", label );
        }

        this.longName = fullName;

        this.measurementUnit = builder.units;

        this.firstCondition = this.getThreshold()
                                  .first()
                                  .getOperator()
                                  .name();
        this.firstDataType = this.getThreshold()
                                 .first()
                                 .getOrientation()
                                 .name();

        if ( this.thresholds.first()
                            .hasLabel() )
        {
            this.eventThresholdName = this.getThreshold()
                                          .first()
                                          .getLabel();
        }
        else
        {
            this.eventThresholdName = NONE;
        }

        // Two thresholds available
        if ( this.thresholds.hasTwo() )
        {
            this.secondCondition = this.getThreshold()
                                       .second()
                                       .getOperator()
                                       .name();
            this.secondDataType = this.getThreshold()
                                      .second()
                                      .getOrientation()
                                      .name();
        }
        else
        {
            this.secondCondition = NONE;
            this.secondDataType = NONE;
        }

        // We only add timing information until we get a lead variable in
        // Use the default duration units
        Number leadLow = Integer.MIN_VALUE;
        Number leadHigh = Integer.MAX_VALUE;

        TimeWindowOuter localWindow = builder.timeWindow;

        if ( !localWindow.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN ) )
        {
            leadLow = DataUtilities.durationToNumericUnits( localWindow.getEarliestLeadDuration(),
                                                            this.getDurationUnits() );
        }
        if ( !localWindow.getLatestLeadDuration().equals( TimeWindowOuter.DURATION_MAX ) )
        {
            leadHigh = DataUtilities.durationToNumericUnits( localWindow.getLatestLeadDuration(),
                                                             this.getDurationUnits() );
        }

        // Long is not supported by netcdf3
        if ( leadLow instanceof Long )
        {
            leadLow = leadLow.intValue();
        }

        if ( leadHigh instanceof Long )
        {
            leadHigh = leadHigh.intValue();
        }

        this.earliestLead = leadLow;
        this.latestLead = leadHigh;

        this.earliestReferenceTime = this.getInstantString( localWindow.getEarliestReferenceTime() );
        this.latestReferenceTime = this.getInstantString( localWindow.getLatestReferenceTime() );
        this.earliestValidTime = this.getInstantString( localWindow.getEarliestValidTime() );
        this.latestValidTime = this.getInstantString( localWindow.getLatestValidTime() );

        // Add the time scale information if available
        TimeScaleOuter localScale = builder.desiredTimeScale;

        this.timeScalePeriod = this.getTimeScalePeriod( localScale );
        this.timeScaleFunction = this.getTimeScaleFunction( localScale );
        this.timeScaleStartMonthDay = this.getTimeScaleStartMonthDay( localScale );
        this.timeScaleEndMonthDay = this.getTimeScaleEndMonthDay( localScale );
    }

    /**
     * @param timeScale the timescale
     * @return the timescale period
     */
    private String getTimeScalePeriod( TimeScaleOuter timeScale )
    {
        if ( Objects.nonNull( timeScale ) )
        {
            if ( timeScale.hasPeriod() )
            {
                // Use the default duration units
                return DataUtilities.durationToNumericUnits( timeScale.getPeriod(),
                                                             this.getDurationUnits() )
                                    .toString();
            }
            else
            {
                return UNDEFINED;
            }
        }

        return UNKNOWN;
    }

    /**
     * @param timeScale the time scale
     * @return the time scale function
     */
    private String getTimeScaleFunction( TimeScaleOuter timeScale )
    {
        if ( Objects.nonNull( timeScale ) )
        {
            return timeScale.getFunction().name();
        }

        return UNKNOWN;
    }

    /**
     * @param timeScale the time scale
     * @return the time scale start month-day
     */
    private String getTimeScaleStartMonthDay( TimeScaleOuter timeScale )
    {
        if ( Objects.nonNull( timeScale ) )
        {
            if ( Objects.nonNull( timeScale.getStartMonthDay() ) )
            {
                return timeScale.getStartMonthDay()
                                .toString();
            }
            else
            {
                return UNDEFINED;
            }
        }

        return UNKNOWN;
    }

    /**
     * @param timeScale the time scale
     * @return the time scale end month-day
     */
    private String getTimeScaleEndMonthDay( TimeScaleOuter timeScale )
    {
        if ( Objects.nonNull( timeScale ) )
        {
            if ( Objects.nonNull( timeScale.getEndMonthDay() ) )
            {
                return timeScale.getEndMonthDay()
                                .toString();
            }
            else
            {
                return UNDEFINED;
            }
        }

        return UNKNOWN;
    }

    private ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }

    private OneOrTwoThresholds getThreshold()
    {
        return this.thresholds;
    }

    private String getInstantString( Instant instant )
    {
        if ( Instant.MIN.equals( instant ) || Instant.MAX.equals( instant ) )
        {
            return "ALL";
        }

        return instant.toString();
    }

}
