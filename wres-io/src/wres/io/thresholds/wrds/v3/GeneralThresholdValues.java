package wres.io.thresholds.wrds.v3;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents threshold values that were formed as part of a calculation.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class GeneralThresholdValues implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6437416253818226237L;

    /** Threshold values. */
    private final Map<String, Double> thresholdValues = new LinkedHashMap<>();

    /** Ratings curve. */
    @JsonProperty( "rating_curve" )
    private RatingCurveInfo ratingCurve;

    /**
     * @return the threshold values
     */
    @JsonAnyGetter
    public Map<String, Double> getThresholdValues()
    {
        return thresholdValues;
    }

    /**
     * Adds a key/value pair.
     * @param key the key
     * @param value the value
     */
    @JsonAnySetter
    public void add( String key, Double value )
    {
        thresholdValues.put( key, value );
    }

    /**
     * @return the rating curve
     */
    public RatingCurveInfo getRatingCurve()
    {
        return ratingCurve;
    }

    /**
     * Sets the rating curve.
     * @param ratingCurve the rating curve
     */
    public void setRatingCurve( RatingCurveInfo ratingCurve )

    {
        this.ratingCurve = ratingCurve;
    }
}
