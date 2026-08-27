package wres.reading.wrds.nwm;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>One element of the top-level WRDS NWM JSON array: a single forecast issuance for one NWM feature and one
 * reference (issuance) datetime, containing one or more ensemble members.
 *
 * <p>A record with exactly one member ({@link #members()} size 1) represents a single-valued time-series; a record
 * with more than one member represents an ensemble time-series, per {@link #isEnsemble()}. This class only captures
 * that structural distinction.
 *
 * @param nwmFeatureId the NWM feature identifier
 * @param configuration the model configuration, e.g. {@code "medium_range"}
 * @param referenceDatetime the forecast reference time
 * @param dataType the type of data, e.g. {@code "streamflow"}
 * @param units the measurement units, e.g. {@code "CMS"}
 * @param members the ensemble members, in the order supplied by the service
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public record WrdsNwmForecastRecord( long nwmFeatureId,
                                     String configuration,
                                     Instant referenceDatetime,
                                     String dataType,
                                     String units,
                                     List<WrdsNwmForecastMember> members )
{
    @JsonCreator
    public WrdsNwmForecastRecord( @JsonProperty( "nwm_feature_id" ) long nwmFeatureId,
                                  @JsonProperty( "configuration" ) String configuration,
                                  @JsonProperty( "reference_datetime" )
                                  @JsonFormat( shape = JsonFormat.Shape.STRING,
                                               pattern = "uuuu-MM-dd'T'HH:mm:ssX" )
                                  Instant referenceDatetime,
                                  @JsonProperty( "data_type" ) String dataType,
                                  @JsonProperty( "units" ) String units,
                                  @JsonProperty( "forecast" ) List<WrdsNwmForecastMember> members )
    {
        this.nwmFeatureId = nwmFeatureId;
        this.configuration = configuration;
        this.referenceDatetime = referenceDatetime;
        this.dataType = dataType;
        this.units = units;
        this.members = List.copyOf( Objects.requireNonNullElse( members, List.of() ) );
    }

    /**
     * @return true if this forecast record has more than one ensemble member, false if it has exactly one
     *         (single-valued) or none
     */

    public boolean isEnsemble()
    {
        return this.members.size() > 1;
    }
}
