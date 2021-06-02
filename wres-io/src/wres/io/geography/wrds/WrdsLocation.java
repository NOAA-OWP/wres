package wres.io.geography.wrds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;

/**
 * For WRDS Location API 2.0 and older, this corresponds directly to an element in
 * the list of "locations".  For WRDS Location API 3.0 and later, this corresponds 
 * to the "identifiers" WITHIN an element in the list of "locations".  
 * @author Hank.Herr
 *
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocation
{
    private final String nwmFeatureId;
    private final String usgsSiteCode;
    private final String nwsLid;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocation( @JsonProperty( "nwm_feature_id" )
                         String nwmFeatureId,
                         @JsonProperty( "usgs_site_code" )
                         String usgsSiteCode,
                         @JsonProperty( "nws_lid" )
                         String nwsLid)
    {
        this.nwmFeatureId = nwmFeatureId;
        this.usgsSiteCode = usgsSiteCode;
        this.nwsLid = nwsLid;
    }

    public String getNwmFeatureId()
    {
        return this.nwmFeatureId;
    }

    public String getUsgsSiteCode()
    {
        return this.usgsSiteCode;
    }

    public String getNwsLid()
    {
        return this.nwsLid;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "nwmFeatureId", nwmFeatureId )
                .append( "usgsSiteCode", usgsSiteCode )
                .append( "nwsLid", nwsLid )
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrdsLocation that = (WrdsLocation) o;
        return Objects.equals(nwmFeatureId, that.nwmFeatureId) &&
                Objects.equals(usgsSiteCode, that.usgsSiteCode) &&
                Objects.equals(nwsLid, that.nwsLid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nwmFeatureId, usgsSiteCode, nwsLid);
    }
}
