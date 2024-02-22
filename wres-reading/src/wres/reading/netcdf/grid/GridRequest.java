package wres.reading.netcdf.grid;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeWindowOuter;

/**
 * A request for gridded data.
 * @param paths the paths
 * @param features the features
 * @param variableName the variable name
 * @param timeWindow the time window
 * @param isForecast whether the request is for forecast data
 * @param timeScale the timescale
 */
public record GridRequest( List<String> paths,
                           Set<Feature> features,
                           String variableName,
                           TimeWindowOuter timeWindow,
                           boolean isForecast,
                           TimeScaleOuter timeScale )
{
    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "variableName",
                                                                                     this.variableName() )
                                                                            .append( "timeWindow",
                                                                                     this.timeWindow() )
                                                                            .append( "isForecast", this.isForecast() )
                                                                            .append( "features", this.features() )
                                                                            .append( "paths", this.paths() )
                                                                            .append( "time scale", this.timeScale() )
                                                                            .toString();
    }

    /**
     * Hidden constructor.
     *
     * @param paths the paths to read, one or more
     * @param features the features to read
     * @param variableName the variable to read
     * @param timeWindow the time window to consider
     * @param isForecast is true if the paths point to forecasts, otherwise false
     * @param timeScale optional time-scale information that can augment, but not override
     * @throws InvalidGridRequestException if the request is invalid for any reason
     */

    public GridRequest( List<String> paths,
                        Set<Feature> features,
                        String variableName,
                        TimeWindowOuter timeWindow,
                        boolean isForecast,
                        TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( paths );
        Objects.requireNonNull( features );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( timeWindow );

        this.paths = Collections.unmodifiableList( paths );
        this.features = Collections.unmodifiableSet( features );
        this.variableName = variableName;
        this.timeWindow = timeWindow;
        this.isForecast = isForecast;
        this.timeScale = timeScale;

        if ( this.paths()
                 .isEmpty() )
        {
            throw new InvalidGridRequestException( "A request for gridded data must contain at least one path to "
                                                   + "read. The request was: " + this );
        }
    }
}
