package wres.config.yaml.components;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationFactory;

/**
 * An interface to use when reading source data.
 * @author James Brown
 */
public enum SourceInterface
{
    /** usgs nwis. */
    @JsonProperty( "usgs nwis" )
    USGS_NWIS( Set.of( DataType.OBSERVATIONS ) ),
    /** "wrds nwm. */
    @JsonProperty( "wrds nwm" )
    WRDS_NWM( Set.of( DataType.SINGLE_VALUED_FORECASTS, DataType.ANALYSES, DataType.ENSEMBLE_FORECASTS ) ),
    /** wrds ahps. */
    @JsonProperty( "wrds ahps" )
    WRDS_AHPS( Set.of( DataType.SINGLE_VALUED_FORECASTS, DataType.OBSERVATIONS ) ),
    /** wrds obs. */
    @JsonProperty( "wrds obs" )
    WRDS_OBS( Set.of( DataType.OBSERVATIONS ) ),
    /** nwm short range channel rt conus. */
    @JsonProperty( "nwm short range channel rt conus" )
    NWM_SHORT_RANGE_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm short range channel rt hawaii. */
    @JsonProperty( "nwm short range channel rt hawaii" )
    NWM_SHORT_RANGE_CHANNEL_RT_HAWAII( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm short range no da channel rt hawaii. */
    @JsonProperty( "nwm short range no da channel rt hawaii" )
    NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_HAWAII( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm short range channel rt puertorico. */
    @JsonProperty( "nwm short range channel rt puertorico" )
    NWM_SHORT_RANGE_CHANNEL_RT_PUERTORICO( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm short range no da channel rt puertorico. */
    @JsonProperty( "nwm short range no da channel rt puertorico" )
    NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_PUERTORICO( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm medium range ensemble channel rt conus. */
    @JsonProperty( "nwm medium range ensemble channel rt conus" )
    NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS( Set.of( DataType.ENSEMBLE_FORECASTS ) ),
    /** nwm medium range deterministic channel rt conus. */
    @JsonProperty( "nwm medium range deterministic channel rt conus" )
    NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm medium range ensemble channel rt conus hourly. */
    @JsonProperty( "nwm medium range ensemble channel rt conus hourly" )
    NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS_HOURLY( Set.of( DataType.ENSEMBLE_FORECASTS ) ),
    /** nwm medium range deterministic channel rt conus hourly. */
    @JsonProperty( "nwm medium range deterministic channel rt conus hourly" )
    NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS_HOURLY( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm medium range no da deterministic channel rt conus. */
    @JsonProperty( "nwm medium range no da deterministic channel rt conus" )
    NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ) ),
    /** nwm analysis assim channel rt conus. */
    @JsonProperty( "nwm analysis assim channel rt conus" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim no da channel rt conus. */
    @JsonProperty( "nwm analysis assim no da channel rt conus" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim extend channel rt conus. */
    @JsonProperty( "nwm analysis assim extend channel rt conus" )
    NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim extend no da channel rt conus. */
    @JsonProperty( "nwm analysis assim extend no da channel rt conus" )
    NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim channel rt hawaii. */
    @JsonProperty( "nwm analysis assim channel rt hawaii" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim no da channel rt hawaii. */
    @JsonProperty( "nwm analysis assim no da channel rt hawaii" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_HAWAII( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim channel rt puertorico. */
    @JsonProperty( "nwm analysis assim channel rt puertorico" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_PUERTORICO( Set.of( DataType.ANALYSES ) ),
    /** nwm analysis assim no da channel rt puertorico. */
    @JsonProperty( "nwm analysis assim no da channel rt puertorico" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO( Set.of( DataType.ANALYSES ) ),
    /** nwm long range channel rt conus. */
    @JsonProperty( "nwm long range channel rt conus" )
    NWM_LONG_RANGE_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ) );

    /** The supported data types. */
    private final Set<DataType> dataTypes;

    /**
     * Creates an instance.
     * @param dataTypes the data types
     */
    SourceInterface( Set<DataType> dataTypes )
    {
        this.dataTypes = dataTypes;
    }

    @Override
    public String toString()
    {
        return DeclarationFactory.getFriendlyName( this.name() );
    }

    /**
     * @return the data type or null if the interface admits several types
     */
    public Set<DataType> getDataTypes()
    {
        return this.dataTypes;
    }
}
