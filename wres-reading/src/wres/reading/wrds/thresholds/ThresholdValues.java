package wres.reading.wrds.thresholds;

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
class ThresholdValues implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6437416253818226237L;

    /** Threshold values. */
    private final Map<String, Double> values = new LinkedHashMap<>();

    /** Ratings curve. */
    @JsonProperty( "rating_curve" )
    private RatingCurveInfo ratingCurve;

    /**
     * @return the threshold values
     */
    @JsonAnyGetter
    Map<String, Double> getThresholdValues()
    {
        return values;
    }

    /**
     * Adds a key/value pair.
     * @param key the key
     * @param value the value
     */
    @JsonAnySetter
    void add( String key, Double value )
    {
        values.put( key, value );
    }

    /**
     * @return the rating curve
     */
    RatingCurveInfo getRatingCurve()
    {
        return ratingCurve;
    }

    /**
     * Sets the rating curve.
     * @param ratingCurve the rating curve
     */
    void setRatingCurve( RatingCurveInfo ratingCurve )

    {
        this.ratingCurve = ratingCurve;
    }
}
