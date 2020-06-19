package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ThresholdMetadata implements Serializable {
    String location_id;
    String id_type;
    String threshold_source;
    String threshold_source_description;
    String rating_source;
    String rating_source_definition;
    String stage_unit;
    String flow_unit;
    Map<String, String> rating;

    String getLocation_id()
    {
        return this.location_id;
    }

    String getId_type()
    {
        return id_type;
    }

    String getThreshold_source()
    {
        if (this.threshold_source.equals("None")) {
            return null;
        }
        return this.threshold_source;
    }

    String getThreshold_source_description()
    {
        return this.threshold_source_description;
    }

    public void setLocation_id(String location_id)
    {
        this.location_id = location_id;
    }

    public void setId_type(String id_type)
    {
        this.id_type = id_type;
    }

    public void setThreshold_source(String threshold_source)
    {
        this.threshold_source = threshold_source;
    }

    public void setThreshold_source_description(String threshold_source_description)
    {
        this.threshold_source_description = threshold_source_description;
    }

    public String getRating_source()
    {
        if (this.rating_source.equals("None")) {
            return null;
        }
        return rating_source;
    }

    public void setRating_source(String rating_source)
    {
        this.rating_source = rating_source;
    }

    public String getRating_source_definition()
    {
        return rating_source_definition;
    }

    public void setRating_source_definition(String rating_source_definition)
    {
        this.rating_source_definition = rating_source_definition;
    }

    public String getStage_unit()
    {
        return stage_unit;
    }

    public void setStage_unit(String stage_unit)
    {
        this.stage_unit = stage_unit;
    }

    public String getFlow_unit()
    {
        return flow_unit;
    }

    public void setFlow_unit(String flow_unit)
    {
        this.flow_unit = flow_unit;
    }

    public Map<String, String> getRating()
    {
        return rating;
    }

    public void setRating(Map<String, String> rating)
    {
        this.rating = rating;
    }
}
