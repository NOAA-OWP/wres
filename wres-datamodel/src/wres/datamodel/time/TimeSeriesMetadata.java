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
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Value object the stores the metadata associated with an {@link TimeSeries}. 
 * 
 * TODO: currently the time-series metadata is inflexible in how it represents geospatial information that is common to
 * all events in the time-series. In practice, a time-series may have no common geospatial information (e.g., because
 * that information is modeled per-event, such as for an environmental tracer or a time-series of inundation extent) or
 * it may have more complex geospatial information than a {@link Feature}. See #96033 for some discussion. One 
 * option would be to add a nullable generic type. This would add flexibility at the expense of additional 
 * parameterization. Another option would be to allow a geospatial representation that contained one of a small number 
 * of possibilities, such as a {@link Feature} or a {@link FeatureTuple}} and admitted an empty representation.
 * Another, simpler, option would be to allow a OneOrTwoFeatures, which covers the typical use case of a univariate or 
 * paired time-series. 
 */

public class TimeSeriesMetadata
{
    /**
     * Cache of commonly used strings.
     */

    private static final Cache<String, String> STRING_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 6 )
                    .build();

    /**
     * Cache of commonly used features. Up to one hundred, arbitrarily.
     */

    private static final Cache<Feature, Feature> FEATURE_KEY_CACHE =
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

    private final Feature feature;

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
                                         Feature feature,
                                         String unit )
    {
        return new Builder().setReferenceTimes( referenceTimes )
                            .setTimeScale( timeScale )
                            .setVariableName( variableName )
                            .setFeature( feature )
                            .setUnit( unit )
                            .build();
    }

    /**
     * @return this instance as a builder.
     */

    public Builder toBuilder()
    {
        return new Builder().setReferenceTimes( this.referenceTimes )
                            .setTimeScale( this.timeScale )
                            .setVariableName( this.variableName )
                            .setFeature( this.feature )
                            .setUnit( this.unit );
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

    public Feature getFeature()
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

        if ( o == null || this.getClass() != o.getClass() )
        {
            return false;
        }

        TimeSeriesMetadata metadata = (TimeSeriesMetadata) o;
        return Objects.equals( this.timeScale, metadata.timeScale ) &&
               this.referenceTimes.equals( metadata.referenceTimes )
               && this.variableName.equals( metadata.variableName )
               && this.feature.equals( metadata.feature )
               && this.unit.equals( metadata.unit );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.timeScale,
                             this.referenceTimes,
                             this.variableName,
                             this.feature,
                             this.unit );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "timeScale", this.timeScale )
                                                                            .append( "referenceTimes",
                                                                                     this.referenceTimes )
                                                                            .append( "variableName", this.variableName )
                                                                            .append( "feature", this.feature )
                                                                            .append( "unit", this.unit )
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

        private Feature feature;


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

        public Builder setReferenceTimes( Map<ReferenceTimeType, Instant> referenceTimes )
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

        public Builder setFeature( Feature feature )
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
            if ( Objects.nonNull( prototype ) )
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
        Map<ReferenceTimeType, Instant> localTimes = builder.referenceTimes;

        if ( Objects.nonNull( localTimes ) )
        {
            localTimes = Collections.unmodifiableMap( localTimes );
        }

        this.referenceTimes = localTimes;
        this.timeScale = builder.timeScale;

        String localVariableName = builder.variableName;
        Feature localFeature = builder.feature;
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

        Feature cachedFeatureKey = FEATURE_KEY_CACHE.getIfPresent( localFeature );

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
