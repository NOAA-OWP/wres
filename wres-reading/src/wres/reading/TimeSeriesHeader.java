package wres.reading;

import java.util.Objects;

import lombok.Builder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;

/**
 * Basic time-series metadata for single-valued or ensemble time-series. The {@link #ensembleMemberIndex()} is
 * ignored by {@link #equals(Object)} and {@link #hashCode()}, which facilitates the aggregation of traces by their
 * common metadata into an ensemble forecast, where applicable.
 *
 * @param locationId the location identifier
 * @param parameterId the parameter identifier
 * @param ensembleId the ensemble identifier
 * @param ensembleMemberIndex the ensemble member index
 * @param forecastDateDate the forecast date
 * @param forecastDateTime the forecast time
 * @param units the measurement units
 * @param moduleInstanceId the module instance identifier
 * @param qualifierId the qualifier identifier
 * @param locationDescription the location description
 * @param type the data type
 * @param timeStepMultiplier the time step multiplier
 * @param timeStepUnit the time step unit
 * @param x the station X coordinate
 * @param y the station Y coordinate
 * @param z the station Z coordinate
 * @param latitude the station latitude
 * @param longitude the station longitude
 * @param missingValue the missing value identifier
 * @param locationName the location name
 * @param locationLongName a long name for the location
 */

@Builder( toBuilder = true )
public record TimeSeriesHeader( String locationId,
                                String parameterId,
                                String ensembleId,
                                String ensembleMemberIndex,
                                String forecastDateDate,
                                String forecastDateTime,
                                String units,
                                String moduleInstanceId,
                                String qualifierId,
                                String locationDescription,
                                String type,
                                String timeStepMultiplier,
                                String timeStepUnit,
                                String x,
                                String y,
                                String z,
                                String latitude,
                                String longitude,
                                String missingValue,
                                String locationName,
                                String locationLongName )
{
    /**
     * Only consider a subset of attributes for comparison.
     *
     * @return the hashcode
     */

    @Override
    public int hashCode()
    {
        return Objects.hash( this.locationId(),
                             this.parameterId(),
                             this.ensembleId(),
                             this.forecastDateDate(),
                             this.forecastDateTime(),
                             this.units(),
                             this.moduleInstanceId(),
                             this.qualifierId(),
                             this.type(),
                             this.timeStepMultiplier(),
                             this.timeStepUnit(),
                             this.missingValue(),
                             this.locationName(),
                             this.locationLongName() );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( !( other instanceof TimeSeriesHeader otherMetadata ) )
        {
            return false;
        }

        return Objects.equals( this.locationId(), otherMetadata.locationId() )
               && Objects.equals( this.parameterId(), otherMetadata.parameterId() )
               && Objects.equals( this.ensembleId(), otherMetadata.ensembleId() )
               && Objects.equals( this.forecastDateDate(), otherMetadata.forecastDateDate() )
               && Objects.equals( this.forecastDateTime(), otherMetadata.forecastDateTime() )
               && Objects.equals( this.units(), otherMetadata.units() )
               && Objects.equals( this.moduleInstanceId(), otherMetadata.moduleInstanceId() )
               && Objects.equals( this.qualifierId(), otherMetadata.qualifierId() )
               && Objects.equals( this.type(), otherMetadata.type() )
               && Objects.equals( this.timeStepMultiplier(), otherMetadata.timeStepMultiplier() )
               && Objects.equals( this.timeStepUnit(), otherMetadata.timeStepUnit() )
               && Objects.equals( this.missingValue(), otherMetadata.missingValue() )
               && Objects.equals( this.locationName(), otherMetadata.locationName() )
               && Objects.equals( this.locationLongName(), otherMetadata.locationLongName() );
    }

    @NotNull
    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "locationId", this.locationId() )
                .append( "parameterId", this.parameterId() )
                .append( "ensembleId", this.ensembleId() )
                .append( "ensembleMemberIndex", this.ensembleMemberIndex() )
                .append( "forecastDateDate", this.forecastDateDate() )
                .append( "forecastDateTime", this.forecastDateTime() )
                .append( "units", this.units() )
                .append( "moduleInstanceId", this.moduleInstanceId() )
                .append( "qualifierId", this.qualifierId() )
                .append( "locationDescription", this.locationDescription() )
                .append( "type", this.type() )
                .append( "timeStepMultiplier", this.timeStepMultiplier() )
                .append( "timeStepUnit", this.timeStepUnit() )
                .append( "x", this.x() )
                .append( "y", this.y() )
                .append( "z", this.z() )
                .append( "latitude", this.latitude() )
                .append( "longitude", this.longitude() )
                .append( "missingValue", this.missingValue() )
                .append( "locationName", this.locationName() )
                .append( "locationLongName", this.locationLongName() )
                .toString();
    }
}
