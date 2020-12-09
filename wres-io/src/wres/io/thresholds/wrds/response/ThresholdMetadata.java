package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ThresholdMetadata implements Serializable {
    String location_id;
    String nws_lid;
    String usgs_site_code;
    String nwm_feature_id;
    String id_type;
    String threshold_source;
    String threshold_source_description;
    String rating_source;
    String rating_source_description;
    String stage_unit;
    String flow_unit;
    Map<String, String> rating;

    public String getLocation_id()
    {
        return this.location_id;
    }

    public String getId_type()
    {
        return id_type;
    }

    public String getThreshold_source()
    {
        if (this.threshold_source.equals("None")) {
            return null;
        }
        return this.threshold_source;
    }

    public String getNws_lid() {
        if (this.nws_lid == null || this.nws_lid.equals("None")) {
            return null;
        }
        return nws_lid;
    }

    public void setNws_lid(String nws_lid) {
        this.nws_lid = nws_lid;
    }

    public String getUsgs_site_code() {
        if (this.usgs_site_code == null || this.usgs_site_code.equals("None")) {
            return null;
        }
        return usgs_site_code;
    }

    public void setUsgs_site_code(String usgs_site_code) {
        this.usgs_site_code = usgs_site_code;
    }

    public String getNwm_feature_id() {
        if (this.nwm_feature_id == null || this.nwm_feature_id.equals("None")) {
            return null;
        }
        return nwm_feature_id;
    }

    public void setNwm_feature_id(String nwm_feature_id) {
        this.nwm_feature_id = nwm_feature_id;
    }

    public String getThreshold_source_description()
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

    public String getRating_source_description()
    {
        return rating_source_description;
    }

    public void setRating_source_description(String rating_source_description)
    {
        this.rating_source_description = rating_source_description;
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
