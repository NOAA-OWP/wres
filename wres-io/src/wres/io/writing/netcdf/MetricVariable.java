package wres.io.writing.netcdf;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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

    /**
     * Builds a new variable.
     * 
     * @param variableName the variable name
     * @param timeWindow the time window
     * @param metricName the metric name
     * @param thresholds the thresholds
     * @param units the measurement units
     * @param desiredTimeScale the desired time scale
     * @param durationUnits the duration units
     */
    
    MetricVariable( String variableName,
                    TimeWindowOuter timeWindow,
                    String metricName,
                    OneOrTwoThresholds thresholds,
                    String units,
                    TimeScaleOuter desiredTimeScale,
                    ChronoUnit durationUnits )
    {
        this.durationUnits = durationUnits;

        this.thresholds = thresholds;

        this.variableName = variableName;

        String fullName = metricName + " " + this.getThreshold();
        fullName = fullName.replaceAll( "\\d+", "#" );

        this.longName = fullName;

        this.measurementUnit = units;
        
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
        if ( thresholds.hasTwo() )
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

        if ( !timeWindow.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN ) )
        {
            leadLow = (int) TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                            this.getDurationUnits() );
        }
        if ( !timeWindow.getLatestLeadDuration().equals( TimeWindowOuter.DURATION_MAX ) )
        {
            leadHigh = (int) TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                             this.getDurationUnits() );
        }
        this.earliestLead = leadLow;
        this.latestLead = leadHigh;

        this.earliestReferenceTime = this.getInstantString( timeWindow.getEarliestReferenceTime() );
        this.latestReferenceTime = this.getInstantString( timeWindow.getLatestReferenceTime() );
        this.earliestValidTime = this.getInstantString( timeWindow.getEarliestValidTime() );
        this.latestValidTime = this.getInstantString( timeWindow.getLatestValidTime() );

        // Add the time scale information if available
        if ( Objects.nonNull( desiredTimeScale ) )
        {
            // Use the default duration units
            this.timeScalePeriod = Long.toString( TimeHelper.durationToLongUnits( desiredTimeScale.getPeriod(),
                                                                                  this.getDurationUnits() ) );
            // Use the enum name()
            this.timeScaleFunction = desiredTimeScale.getFunction().name();
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

        Map<String, Object> attributes = new TreeMap<>(  );

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
        if( Instant.MIN.equals( instant ) || Instant.MAX.equals( instant ) )
        {
            return "ALL";
        }
        
        return instant.toString();
    }
    
}
