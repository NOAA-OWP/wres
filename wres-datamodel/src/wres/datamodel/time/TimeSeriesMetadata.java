package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;

/**
 * Value object the stores the metadata associated with an {@link TimeSeries}.
 */

public class TimeSeriesMetadata
{
    /**
     * Use six because the common case would be up to three variable names and
     * up to three unit names, one for each of L/R/B.
     */

    private static final Cache<String,String> STRING_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 6 )
                    .build();

    /**
     * Use one hundred arbitrarily. There are usually more than a handful.
     */

    private static final Cache<FeatureKey,FeatureKey> FEATURE_KEY_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 100 )
                    .build();

    /**
     * The {@link TimeScaleOuter} associated with the events in the time-series.
     */

    private final TimeScaleOuter timeScale;


    /**
     * The zero or more reference datetimes associated with the time-series.
     */

    private final Map<ReferenceTimeType, Instant> referenceTimes;


    /**
     * The name of the variable represented, as found in the original dataset.
     */

    private final String variableName;


    /**
     * The geographic feature.
     */

    private final FeatureKey feature;


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
     * @param feature The feature, non-null.
     * @param unit The measurement unit name, non-null.
     * @return an instance
     * @throws NullPointerException if any arg besides timeScale is null
     */

    public static TimeSeriesMetadata of( Map<ReferenceTimeType, Instant> referenceTimes,
                                         TimeScaleOuter timeScale,
                                         String variableName,
                                         FeatureKey feature,
                                         String unit )
    {
        return new Builder().setReferenceTimes( referenceTimes )
                            .setTimeScale( timeScale )
                            .setVariableName( variableName )
                            .setFeature( feature )
                            .setUnit( unit )
                            .build();
    }

    public TimeScaleOuter getTimeScale()
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

    public FeatureKey getFeature()
    {
        return this.feature;
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
               feature.equals( metadata.feature ) &&
               unit.equals( metadata.unit );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( timeScale,
                             referenceTimes,
                             variableName,
                             feature,
                             unit );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "timeScale", timeScale )
                                                                            .append( "referenceTimes", referenceTimes )
                                                                            .append( "variableName", variableName )
                                                                            .append( "feature", feature )
                                                                            .append( "unit", unit )
                                                                            .toString();
    }
    
    /**
     * Metadata builder.
     */
    
    public static class Builder 
    {
        /**
         * The {@link TimeScaleOuter} associated with the events in the time-series.
         */

        private TimeScaleOuter timeScale;

        /**
         * The zero or more reference datetimes associated with the time-series.
         */

        private Map<ReferenceTimeType, Instant> referenceTimes;


        /**
         * The name of the variable represented, as found in the original dataset.
         */

        private String variableName;


        /**
         * The geographic feature.
         */

        private FeatureKey feature;


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
        
        public Builder setTimeScale( TimeScaleOuter timeScale )
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
         * Sets the feature.
         *
         * @param feature the feature
         * @return the builder
         */
        
        public Builder setFeature( FeatureKey feature )
        {
            this.feature = feature;
            
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
                this.setFeature( prototype.getFeature() );
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

        String localVariableName = builder.variableName;
        FeatureKey localFeature = builder.feature;
        String localUnits = builder.unit;
        
        Objects.requireNonNull( localVariableName, "A timeseries requires a variable name." );
        Objects.requireNonNull( localFeature, "A timeseries requires a feature." );
        Objects.requireNonNull( localUnits, "A timeseries requires measurement units." );
        
        String cachedVariableName = STRING_CACHE.getIfPresent( localVariableName );

        if ( Objects.nonNull( cachedVariableName ) )
        {
            this.variableName = cachedVariableName;
        }
        else
        {
            this.variableName = localVariableName;
            STRING_CACHE.put( this.variableName, this.variableName );
        }

        FeatureKey cachedFeatureKey = FEATURE_KEY_CACHE.getIfPresent( localFeature );

        if ( Objects.nonNull( cachedFeatureKey ) )
        {
            this.feature = cachedFeatureKey;
        }
        else
        {
            this.feature = localFeature;
            FEATURE_KEY_CACHE.put( this.feature, this.feature );
        }

        String cachedUnit = STRING_CACHE.getIfPresent( localUnits );

        if ( Objects.nonNull( cachedUnit ) )
        {
            this.unit = cachedUnit;
        }
        else
        {
            this.unit = localUnits;
            STRING_CACHE.put( this.unit, this.unit );
        }
        
        Objects.requireNonNull( this.getReferenceTimes() );
        // Often the timescale is not available: in that case valid to use null.
        Objects.requireNonNull( this.getVariableName() );
        Objects.requireNonNull( this.getFeature() );
        Objects.requireNonNull( this.getUnit() );

    }
}
