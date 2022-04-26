package wres.io.writing.netcdf;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import wres.config.generated.EnsembleAverageType;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.util.TimeHelper;

class MetricVariable
{

    private static final String NONE = "NONE";

    private final ChronoUnit durationUnits;

    private final String variableName;
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

    private final OneOrTwoThresholds thresholds;

    // TODO: make these longs
    // Chris Tubbs: We can only write Netcdf-3, and Netcdf-3 doesn't support longs. Keeping them
    // as 'int's here helps us avoid potential value conversion issues down the line
    private final int earliestLead;
    private final int latestLead;

    private final EnsembleAverageType ensembleAverageType;

    /**
     * Builder.
     */
    static class Builder
    {
        private String variableName;
        private TimeWindowOuter timeWindow;
        private String metricName;
        private OneOrTwoThresholds thresholds;
        private String units;
        private TimeScaleOuter desiredTimeScale;
        private ChronoUnit durationUnits;
        private EnsembleAverageType ensembleAverageType;

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

        Builder setMetricName( String metricName )
        {
            this.metricName = metricName;
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

        Builder setEnsembleAverageType( EnsembleAverageType ensembleAverageType )
        {
            this.ensembleAverageType = ensembleAverageType;
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

        // Build the long_name. This should not contain numbers for the thresholds because they can vary by feature.
        String fullName = builder.metricName + " " + this.getThreshold();

        // Primary threshold has a label? If so, ignore that when replacing numbers in the long_name.
        // See #85465.
        if ( this.thresholds.first().hasLabel() )
        {
            String label = this.thresholds.first().getLabel();
            // Replace with a temporary placeholder that contains no numbers
            fullName = fullName.replace( label, "oHhcAqApZQLTLmtDvaX" );
        }

        // Replace all numbers with a "variable number" placeholder because these numbers can vary by feature. A more 
        // sophisticated approach would determine whether they do actually vary by feature and only replace them in that 
        // case. The CF convention is silent about variable names for thresholds that vary by feature. Some related 
        // discussion here: https://is.enes.org/archive-1/phase-2/documents/Talks/2nd-joint-clipc-is-enes2-workshop-on-metadata-drs-for-climate-indices/l-barring_data-variable-attributes-for-climate-indices
        fullName = fullName.replaceAll( "\\d+", "#" );

        // Add back any label removed.
        if ( this.thresholds.first().hasLabel() )
        {
            String label = thresholds.first().getLabel();
            fullName = fullName.replace( "oHhcAqApZQLTLmtDvaX", label );
        }

        this.longName = fullName;

        this.measurementUnit = builder.units;

        this.firstCondition = this.getThreshold().first().getOperator().name();
        this.firstDataType = this.getThreshold().first().getDataType().name();

        if ( this.thresholds.first().hasLabel() )
        {
            this.eventThresholdName = this.getThreshold().first().getLabel();
        }
        else
        {
            this.eventThresholdName = NONE;
        }

        // Two thresholds available
        if ( this.thresholds.hasTwo() )
        {
            this.secondCondition = this.getThreshold().second().getOperator().name();
            this.secondDataType = this.getThreshold().second().getDataType().name();
        }
        else
        {
            this.secondCondition = NONE;
            this.secondDataType = NONE;
        }

        // We only add timing information until we get a lead variable in
        // Use the default duration units
        int leadLow = Integer.MIN_VALUE;
        int leadHigh = Integer.MAX_VALUE;

        TimeWindowOuter localWindow = builder.timeWindow;

        if ( !localWindow.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN ) )
        {
            leadLow = (int) TimeHelper.durationToLongUnits( localWindow.getEarliestLeadDuration(),
                                                            this.getDurationUnits() );
        }
        if ( !localWindow.getLatestLeadDuration().equals( TimeWindowOuter.DURATION_MAX ) )
        {
            leadHigh = (int) TimeHelper.durationToLongUnits( localWindow.getLatestLeadDuration(),
                                                             this.getDurationUnits() );
        }
        this.earliestLead = leadLow;
        this.latestLead = leadHigh;

        this.earliestReferenceTime = this.getInstantString( localWindow.getEarliestReferenceTime() );
        this.latestReferenceTime = this.getInstantString( localWindow.getLatestReferenceTime() );
        this.earliestValidTime = this.getInstantString( localWindow.getEarliestValidTime() );
        this.latestValidTime = this.getInstantString( localWindow.getLatestValidTime() );

        // Add the time scale information if available
        TimeScaleOuter localScale = builder.desiredTimeScale;
        if ( Objects.nonNull( localScale ) )
        {
            // Use the default duration units
            this.timeScalePeriod = Long.toString( TimeHelper.durationToLongUnits( localScale.getPeriod(),
                                                                                  this.getDurationUnits() ) );
            // Use the enum name()
            this.timeScaleFunction = localScale.getFunction().name();
        }
        else
        {
            this.timeScalePeriod = "UNKNOWN";
            this.timeScaleFunction = "UNKNOWN";
        }
    }

    public String getName()
    {
        return this.variableName;
    }

    Map<String, Object> getAttributes()
    {

        Map<String, Object> attributes = new TreeMap<>();

        attributes.put( "earliest_reference_time", this.earliestReferenceTime );
        attributes.put( "latest_reference_time", this.latestReferenceTime );
        attributes.put( "earliest_valid_time", this.earliestValidTime );
        attributes.put( "latest_valid_time", this.latestValidTime );

        // Add the default duration units to qualify
        attributes.put( "earliest_lead_" + this.getDurationUnits().name().toLowerCase(),
                        this.earliestLead );
        attributes.put( "latest_lead_" + this.getDurationUnits().name().toLowerCase(),
                        this.latestLead );
        attributes.put( "event_threshold_data_type", this.firstDataType );
        attributes.put( "decision_threshold_data_type", this.secondDataType );
        attributes.put( "event_threshold_name", this.eventThresholdName );
        attributes.put( "measurement_unit", this.measurementUnit );
        attributes.put( "long_name", this.longName );
        attributes.put( "event_threshold_condition", this.firstCondition );
        attributes.put( "decision_threshold_condition", this.secondCondition );

        // Add the default duration units to qualify
        attributes.put( "time_scale_period_" + this.getDurationUnits().name().toLowerCase(),
                        this.timeScalePeriod );
        attributes.put( "time_scale_function", this.timeScaleFunction );
        attributes.put( "ensemble_average_type", this.ensembleAverageType.name() );

        return attributes;
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
