package wres.grid.client;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;

/**
 * A response that wraps a stream of time-series per feature with other metadata.
 * 
 * TODO: eliminate this wrapper and directly return the time-series per feature with inline metadata from the 
 * {@link Fetcher}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedTimeSeriesResponse implements Response<Double>
{

    /**
     * The measurement units.
     */

    private final String measurementUnits;

    /**
     * The variable name.
     */

    private final String variableName;

    /**
     * The time-series per feature.
     */

    private final Map<FeatureKey, Stream<TimeSeries<Double>>> timeSeriesPerFeature;

    @Override
    public String getVariableName()
    {
        return this.variableName;
    }

    @Override
    public Map<FeatureKey, Stream<TimeSeries<Double>>> getTimeSeries()
    {
        return this.timeSeriesPerFeature; // Rendered immutable on construction
    }

    @Override
    public String getMeasuremenUnits()
    {
        return this.measurementUnits;
    }
    
    /**
     * Returns an instance.
     * 
     * @param timeSeriesPerFeature the time-series per feature
     * @param variableName the variable name
     * @param measurementUnits the measurement units
     * @return an instance
     * @throws NullPointerException if any input is null
     */

    public static SingleValuedTimeSeriesResponse of( Map<FeatureKey, Stream<TimeSeries<Double>>> timeSeriesPerFeature,
                                                     String variableName,
                                                     String measurementUnits )
    {
        return new SingleValuedTimeSeriesResponse( timeSeriesPerFeature, variableName, measurementUnits );
    }
    
    /**
     * Hidden constructor.
     * 
     * @param timeSeriesPerFeature the time-series per feature
     * @param variableName the variable name
     * @param measurementUnits the measurement units
     * @throws NullPointerException if any input is null
     */

    private SingleValuedTimeSeriesResponse( Map<FeatureKey, Stream<TimeSeries<Double>>> timeSeriesPerFeature,
                                            String variableName,
                                            String measurementUnits )
    {
        Objects.requireNonNull( timeSeriesPerFeature );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( measurementUnits );

        this.variableName = variableName;
        this.measurementUnits = measurementUnits;

        // Render the time-series per feature immutable
        this.timeSeriesPerFeature = Collections.unmodifiableMap( timeSeriesPerFeature );
    }


}
