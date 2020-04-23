package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.scale.TimeScale;

/**
 * Value object the stores the metadata associated with an {@link TimeSeries}.
 */

public class TimeSeriesMetadata
{
    /**
     * The {@link TimeScale} associated with the events in the time-series.
     */

    private final TimeScale timeScale;


    /**
     * The zero or more reference datetimes associated with the time-series.
     */

    private final Map<ReferenceTimeType, Instant> referenceTimes;


    /**
     * The name of the variable represented, as found in the original dataset.
     */

    private final String variableName;


    /**
     * The name of the geographic feature.
     */

    private final String featureName;


    /**
     * The name of the unit of measure.
     */

    private final String unit;

    /**
     * Create an instance.
     *
     * @param referenceTimes The reference times, non-null
     * @param timeScale The time scale if available, or null if not.
     * @param variableName The variable name, non-null.
     * @param featureName The feature name, non-null.
     * @param unit The measurement unit name, non-null.
     * @return an instance
     * @throws NullPointerException if any arg besides timeScale is null
     */

    public static TimeSeriesMetadata of( Map<ReferenceTimeType, Instant> referenceTimes,
                                         TimeScale timeScale,
                                         String variableName,
                                         String featureName,
                                         String unit )
    {
        return new Builder().setReferenceTimes( referenceTimes )
                            .setTimeScale( timeScale )
                            .setVariableName( variableName )
                            .setFeatureName( featureName )
                            .setUnit( unit )
                            .build();
    }

    public TimeScale getTimeScale()
    {
        return this.timeScale;
    }

    public Map<ReferenceTimeType, Instant> getReferenceTimes()
    {
        return this.referenceTimes;
    }

    public String getVariableName()
    {
        return this.variableName;
    }

    public String getFeatureName()
    {
        return this.featureName;
    }

    public String getUnit()
    {
        return this.unit;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TimeSeriesMetadata metadata = ( TimeSeriesMetadata ) o;
        return Objects.equals( timeScale, metadata.timeScale ) &&
               referenceTimes.equals( metadata.referenceTimes ) &&
               variableName.equals( metadata.variableName ) &&
               featureName.equals( metadata.featureName ) &&
               unit.equals( metadata.unit );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( timeScale,
                             referenceTimes,
                             variableName,
                             featureName,
                             unit );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "timeScale", timeScale )
                                                                            .append( "referenceTimes", referenceTimes )
                                                                            .append( "variableName", variableName )
                                                                            .append( "featureName", featureName )
                                                                            .append( "unit", unit )
                                                                            .toString();
    }
    
    /**
     * Metadata builder.
     */
    
    public static class Builder 
    {
        /**
         * The {@link TimeScale} associated with the events in the time-series.
         */

        private TimeScale timeScale;

        /**
         * The zero or more reference datetimes associated with the time-series.
         */

        private Map<ReferenceTimeType, Instant> referenceTimes;


        /**
         * The name of the variable represented, as found in the original dataset.
         */

        private String variableName;


        /**
         * The name of the geographic feature.
         */

        private String featureName;


        /**
         * The name of the unit of measure.
         */

        private String unit;
        
        /**
         * Sets the time-scale.
         * 
         * @param timeScale the time-scale
         * @return the builder
         */
        
        public Builder setTimeScale( TimeScale timeScale )
        {
            this.timeScale = timeScale;
            
            return this;
        }
        
        /**
         * Sets the time-scale.
         * 
         * @param referenceTimes the reference times (zero or more)
         * @return the builder
         */
        
        public Builder setReferenceTimes( Map<ReferenceTimeType,Instant> referenceTimes )
        {
            this.referenceTimes = referenceTimes;
            
            return this;
        }
        
        /**
         * Sets the variable name.
         *
         * @param variableName the variable name
         * @return the builder
         */
        
        public Builder setVariableName( String variableName )
        {
            this.variableName = variableName;
            
            return this;
        }
        
        /**
         * Sets the feature name.
         *
         * @param featureName the feature name
         * @return the builder
         */
        
        public Builder setFeatureName( String featureName )
        {
            this.featureName = featureName;
            
            return this;
        }
        
        /**
         * Sets the measurement unit.
         * 
         * @param unit the measurement unit
         * @return the builder
         */
        
        public Builder setUnit( String unit )
        {
            this.unit = unit;
            
            return this;
        }
        
        /**
         * Builds an instance of the metadata.
         * 
         * @return the metadata
         */
        
        public TimeSeriesMetadata build()
        {
            return new TimeSeriesMetadata( this );
        }
        
        /**
         * Default constructor.
         */
        
        public Builder()
        {            
        }
        
        /**
         * Creates a builder initialized with a prototype.
         * 
         * @param prototype the prototype metadata
         */
        
        public Builder( TimeSeriesMetadata prototype )
        {
            if( Objects.nonNull( prototype ) )
            {
                this.setFeatureName( prototype.getFeatureName() );
                this.setVariableName( prototype.getVariableName() );
                this.setReferenceTimes( prototype.getReferenceTimes() );
                this.setTimeScale( prototype.getTimeScale() );
                this.setUnit( prototype.getUnit() );
            }
        }        
        
    }
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws NullPointerException if any arg besides timeScale is null
     */

    private TimeSeriesMetadata( Builder builder )
    {
        // Set then validate
        Map<ReferenceTimeType,Instant> localTimes = builder.referenceTimes;
        
        if( Objects.nonNull( localTimes ) )
        {
            localTimes = Collections.unmodifiableMap( localTimes );
        }
        
        this.referenceTimes = localTimes;
        this.timeScale = builder.timeScale;
        this.variableName = builder.variableName;
        this.featureName = builder.featureName;
        this.unit = builder.unit;
        
        Objects.requireNonNull( this.getReferenceTimes() );
        // Often the timescale is not available: in that case valid to use null.
        Objects.requireNonNull( this.getVariableName() );
        Objects.requireNonNull( this.getFeatureName() );
        Objects.requireNonNull( this.getUnit() );

    }
}
