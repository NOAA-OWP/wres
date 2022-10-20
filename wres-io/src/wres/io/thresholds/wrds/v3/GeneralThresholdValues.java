package wres.io.thresholds.wrds.v3;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents threshold values that were formed as part of a calculation
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GeneralThresholdValues implements Serializable 
{

    private static final long serialVersionUID = 6437416253818226237L;
    private Map<String, Double> tresholdValues = new LinkedHashMap<>();
    
    RatingCurveInfo rating_curve;
    
    @JsonAnyGetter
    public Map<String, Double> getThresholdValues()
    {
        return tresholdValues;
    }
    
    @JsonAnySetter
    public void add(String key, Double value)
    {
        tresholdValues.put( key,  value );
    }
    
    public RatingCurveInfo getRating_curve()
    {
        return rating_curve;
    }
    
    public void setRating_curve(RatingCurveInfo rating_curve)

    {
        this.rating_curve = rating_curve;
    }
    
}
