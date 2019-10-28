package wres.grid.client;

import java.util.List;

import wres.config.generated.Feature;
import wres.datamodel.time.TimeWindow;

/**
 * Prototype Interface for requesting grid data
 */
public interface Request
{
    /**
     * @return the paths to gridded data files
     */
    
    List<String> getPaths();
    
    /**
     * @return the features requested
     */
    
    List<Feature> getFeatures();
    
    /**
     * @return the time window
     */
    
    TimeWindow getTimeWindow();
    
    /**
     * @return the variable name
     */
    
    String getVariableName();
    
    /**
     * @return true if the request involves forecasts, false otherwise
     */
    
    boolean isForecast();
}
