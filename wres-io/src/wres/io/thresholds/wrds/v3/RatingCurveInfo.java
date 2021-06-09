package wres.io.thresholds.wrds.v3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingCurveInfo implements Serializable
{
    String location_id;
    String id_type;
    String source;
    String description;
    String interpolation_method;
    String interpolation_description;

    public String getLocation_id()
    {
        return this.location_id;
    }

    public String getId_type()
    {
        return id_type;
    }

    public String getSource()
    {
        if ( Objects.isNull( this.source ) || this.source.equals( "None" ) )
        {
            return null;
        }
        return this.source;
    }

    public String getDescription()
    {
        return this.description;
    }
    
    public String getInterpolation_method()
    {
        return this.interpolation_method;
    }
    
    public String getInterplolation_description()
    {
        return this.interpolation_description;
    }

    public void setLocation_id(String location_id)
    {
        this.location_id = location_id;
    }

    public void setId_type(String id_type)
    {
        this.id_type = id_type;
    }

    public void setSource(String source)
    {
        this.source = source;
    }
    
    public void setDescription(String description)
    {
        this.description = description;
    }
}
