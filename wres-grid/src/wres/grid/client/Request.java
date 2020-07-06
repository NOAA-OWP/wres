package wres.grid.client;

import java.util.List;

import wres.config.generated.Feature;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeWindowOuter;

import java.nio.file.Path;

/**
 * Prototype Interface for requesting grid data
 */

public interface Request
{
    /**
     * TODO: please return a list of {@link Path}. Would prefer that, but didn't try too hard at the time of writing
     * because the SourceLoader was having issues translating URI to Path. Post #63470, this may be easier to achieve. 
     * 
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
    
    TimeWindowOuter getTimeWindow();
    
    /**
     * @return the variable name
     */
    
    String getVariableName();
    
    /**
     * @return the declared existing time scale, if any
     */
    
    TimeScale getTimeScale();
    
    /**
     * @return true if the request involves forecasts, false otherwise
     */
    
    boolean isForecast();
}
