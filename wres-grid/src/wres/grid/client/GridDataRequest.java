package wres.grid.client;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import wres.config.generated.Feature;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeWindow;

/**
 * A request for gridded data.
 */

class GridDataRequest implements Request
{

    /**
     * The paths to read.
     */

    private final List<String> paths;

    /**
     * The features to read.
     */

    private final List<Feature> features;

    /**
     * The variable name.
     */

    private final String variableName;

    /**
     * The time window to consider.
     */
    private final TimeWindow timeWindow;

    /**
     * Is <code>true</code> if the paths point to forecasts, <code>false</code> otherwise.
     */

    private final boolean isForecast;
    
    /**
     * Optional time-scale information that can augment a source, but not override it.
     */
    
    private final TimeScale declaredExistingTimeScale;

    /**
     * Returns an instance.
     * 
     * @param paths the paths to read
     * @param features the features to read
     * @param variableName the variable to read
     * @param timeWindow the time window to consider
     * @param isForecast is true if the paths point to forecasts, otherwise false
     * @param declaredExistingTimeScale optional time-scale information that can augment, but not override
     * @return an instance
     * @throws NullPointerException if any nullable input is null
     */

    static GridDataRequest of( List<String> paths,
                               List<Feature> features,
                               String variableName,
                               TimeWindow timeWindow,
                               boolean isForecast,
                               TimeScale declaredExistingTimeScale )
    {
        return new GridDataRequest( paths, features, variableName, timeWindow, isForecast, declaredExistingTimeScale );
    }

    /**
     * Hidden constructor.
     * 
     * @param paths the paths to read
     * @param features the features to read
     * @param variableName the variable to read
     * @param timeWindow the time window to consider
     * @param isForecast is true if the paths point to forecasts, otherwise false
     * @param declaredExistingTimeScale optional time-scale information that can augment, but not override
     */

    private GridDataRequest( List<String> paths,
                             List<Feature> features,
                             String variableName,
                             TimeWindow timeWindow,
                             boolean isForecast,
                             TimeScale declaredExistingTimeScale )
    {
        Objects.requireNonNull( paths );
        Objects.requireNonNull( features );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( timeWindow );

        this.paths = Collections.unmodifiableList( paths );
        this.features = Collections.unmodifiableList( features );
        this.variableName = variableName;
        this.timeWindow = timeWindow;
        this.isForecast = isForecast;
        this.declaredExistingTimeScale = declaredExistingTimeScale;
    }

    @Override
    public String getVariableName()
    {
        return this.variableName;
    }

    @Override
    public boolean isForecast()
    {
        return this.isForecast;
    }

    @Override
    public List<String> getPaths()
    {
        return this.paths; // Rendered immutable on construction
    }

    @Override
    public List<Feature> getFeatures()
    {
        return this.features; // Rendered immutable on construction
    }

    @Override
    public TimeWindow getTimeWindow()
    {
        return this.timeWindow;
    }
    
    @Override
    public TimeScale getTimeScale()
    {
        return this.declaredExistingTimeScale;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );

        joiner.add( "GridDataRequest instance " + this.hashCode() + ": { " );
        joiner.add( "    variableName: " + this.getVariableName() + "," );
        joiner.add( "    timeWindow: " + this.getTimeWindow() + "," );
        joiner.add( "    isForecast: " + this.isForecast() + "," );
        joiner.add( "    features: " + this.getFeatures() + "," );
        joiner.add( "    paths: " + this.getPaths() );
        joiner.add( "    time scale: " + this.getTimeScale() );
        joiner.add( "}" );

        return joiner.toString();
    }

}
