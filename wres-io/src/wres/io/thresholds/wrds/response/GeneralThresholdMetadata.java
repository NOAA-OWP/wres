package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GeneralThresholdMetadata implements Serializable 
{

    String data_type;
    String nws_lid;
    String usgs_site_code;
    String nwm_feature_id;
    String threshold_type;
    String threshold_source;
    String threshold_source_description;
    String stage_units;
    String flow_units;
    String calc_flow_units;
    String units;
    
    public String getData_type()
    {
        return data_type;
    }
    public void setData_type( String data_type )
    {
        this.data_type = data_type;
    }
    public String getNws_lid()
    {
        return nws_lid;
    }
    public void setNws_lid( String nws_lid )
    {
        this.nws_lid = nws_lid;
    }
    public String getUsgs_site_code()
    {
        return usgs_site_code;
    }
    public void setUsgs_site_code( String usgs_site_code )
    {
        this.usgs_site_code = usgs_site_code;
    }
    public String getNwm_feature_id()
    {
        return nwm_feature_id;
    }
    public void setNwm_feature_id( String nwm_feature_id )
    {
        this.nwm_feature_id = nwm_feature_id;
    }
    public String getThreshold_type()
    {
        return threshold_type;
    }
    public void setThreshold_type( String threshold_type )
    {
        this.threshold_type = threshold_type;
    }
    public String getThreshold_source()
    {
        return threshold_source;
    }
    public void setThreshold_source( String threshold_source )
    {
        this.threshold_source = threshold_source;
    }
    public String getThreshold_source_description()
    {
        return threshold_source_description;
    }
    public void setThreshold_source_description( String threshold_source_description )
    {
        this.threshold_source_description = threshold_source_description;
    }
    public String getStage_units()
    {
        return stage_units;
    }
    public void setStage_units( String stage_units )
    {
        this.stage_units = stage_units;
    }
    public String getFlow_units()
    {
        return flow_units;
    }
    public void setFlow_units( String flow_units )
    {
        this.flow_units = flow_units;
    }
    public String getCalc_flow_units()
    {
        return calc_flow_units;
    }
    public void setCalc_flow_units( String calc_flow_units )
    {
        this.calc_flow_units = calc_flow_units;
    }
    public String getUnits()
    {
        return units;
    }
    public void setUnits( String units )
    {
        this.units = units;
    }


    
}
