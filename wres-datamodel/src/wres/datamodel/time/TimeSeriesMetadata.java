package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.scale.TimeScale;

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

    public static TimeSeriesMetadata of( Map<ReferenceTimeType,Instant> referenceTimes,
                                         TimeScale timeScale,
                                         String variableName,
                                         String featureName,
                                         String unit )
    {
        return new TimeSeriesMetadata( referenceTimes,
                                       timeScale,
                                       variableName,
                                       featureName,
                                       unit );
    }

    private TimeSeriesMetadata( Map<ReferenceTimeType,Instant> referenceTimes,
                                TimeScale timeScale,
                                String variableName,
                                String featureName,
                                String unit )
    {
        Objects.requireNonNull( referenceTimes );
        this.referenceTimes = Collections.unmodifiableMap( referenceTimes );
        this.timeScale = timeScale;
        this.variableName = variableName;
        this.featureName = featureName;
        this.unit = unit;
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
               Objects.equals( variableName, metadata.variableName ) &&
               Objects.equals( featureName, metadata.featureName ) &&
               Objects.equals( unit, metadata.unit );
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
}
