package wres.grid.client;

import java.util.Map;
import java.util.stream.Stream;

import wres.config.FeaturePlus;
import wres.datamodel.time.TimeSeries;

/**
 * Prototype interface for receiving grid information
 */
public interface Response<T>
{
    /**
     * Returns the time-series per feature.
     * 
     * @return a stream of time-series per feature
     */
    Map<FeaturePlus, Stream<TimeSeries<T>>> getTimeSeries();

    /**
     * The measurement units
     * 
     * @return the measurement units
     */
    String getMeasuremenUnits();

    /**
     * The variable name
     * 
     * @return the variable name
     */
    
    String getVariableName();
}
