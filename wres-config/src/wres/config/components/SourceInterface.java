package wres.config.components;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * An interface to use when reading source data.
 * @author James Brown
 */
public enum SourceInterface
{
    /** usgs nwis. */
    @JsonProperty( "usgs nwis" )
    USGS_NWIS( Set.of( DataType.OBSERVATIONS ), FeatureAuthority.USGS_SITE_CODE ),
    /** "wrds nwm. */
    @JsonProperty( "wrds nwm" )
    WRDS_NWM( Set.of( DataType.SINGLE_VALUED_FORECASTS, DataType.ANALYSES, DataType.ENSEMBLE_FORECASTS ),
              FeatureAuthority.NWM_FEATURE_ID ),
    /** wrds ahps. */
    @JsonProperty( "wrds ahps" )
    WRDS_AHPS( Set.of( DataType.SINGLE_VALUED_FORECASTS, DataType.OBSERVATIONS ), FeatureAuthority.NWS_LID ),
    /** wrds hefs. */
    @JsonProperty( "wrds hefs" )
    WRDS_HEFS( Set.of( DataType.ENSEMBLE_FORECASTS ), FeatureAuthority.NWS_LID ),
    /** wrds obs. */
    @JsonProperty( "wrds obs" )
    WRDS_OBS( Set.of( DataType.OBSERVATIONS ), FeatureAuthority.NWS_LID ),
    /** nwm short range channel rt conus. */
    @JsonProperty( "nwm short range channel rt conus" )
    NWM_SHORT_RANGE_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm short range channel rt hawaii. */
    @JsonProperty( "nwm short range channel rt hawaii" )
    NWM_SHORT_RANGE_CHANNEL_RT_HAWAII( Set.of( DataType.SINGLE_VALUED_FORECASTS ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm short range no da channel rt hawaii. */
    @JsonProperty( "nwm short range no da channel rt hawaii" )
    NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_HAWAII( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                             FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm short range channel rt puertorico. */
    @JsonProperty( "nwm short range channel rt puertorico" )
    NWM_SHORT_RANGE_CHANNEL_RT_PUERTORICO( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                           FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm short range no da channel rt puertorico. */
    @JsonProperty( "nwm short range no da channel rt puertorico" )
    NWM_SHORT_RANGE_NO_DA_CHANNEL_RT_PUERTORICO( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                 FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range ensemble channel rt conus. */
    @JsonProperty( "nwm medium range ensemble channel rt conus" )
    NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS( Set.of( DataType.ENSEMBLE_FORECASTS ),
                                                FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range deterministic channel rt conus. */
    @JsonProperty( "nwm medium range deterministic channel rt conus" )
    NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                     FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range ensemble channel rt conus hourly. */
    @JsonProperty( "nwm medium range ensemble channel rt conus hourly" )
    NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_CONUS_HOURLY( Set.of( DataType.ENSEMBLE_FORECASTS ),
                                                       FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range deterministic channel rt conus hourly. */
    @JsonProperty( "nwm medium range deterministic channel rt conus hourly" )
    NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_CONUS_HOURLY( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                            FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range no da deterministic channel rt conus. */
    @JsonProperty( "nwm medium range no da deterministic channel rt conus" )
    NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                           FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim channel rt conus. */
    @JsonProperty( "nwm analysis assim channel rt conus" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim no da channel rt conus. */
    @JsonProperty( "nwm analysis assim no da channel rt conus" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim extend channel rt conus. */
    @JsonProperty( "nwm analysis assim extend channel rt conus" )
    NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim extend no da channel rt conus. */
    @JsonProperty( "nwm analysis assim extend no da channel rt conus" )
    NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_CONUS( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim channel rt hawaii. */
    @JsonProperty( "nwm analysis assim channel rt hawaii" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_HAWAII( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim no da channel rt hawaii. */
    @JsonProperty( "nwm analysis assim no da channel rt hawaii" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_HAWAII( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim channel rt puertorico. */
    @JsonProperty( "nwm analysis assim channel rt puertorico" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_PUERTORICO( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim no da channel rt puertorico. */
    @JsonProperty( "nwm analysis assim no da channel rt puertorico" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_PUERTORICO( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm long range channel rt conus. */
    @JsonProperty( "nwm long range channel rt conus" )
    NWM_LONG_RANGE_CHANNEL_RT_CONUS( Set.of( DataType.SINGLE_VALUED_FORECASTS ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm short range channel rt alaska. */
    @JsonProperty( "nwm short range channel rt alaska" )
    NWM_SHORT_RANGE_CHANNEL_RT_CONUS_ALASKA( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                             FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range ensemble channel rt alaska. */
    @JsonProperty( "nwm medium range ensemble channel rt alaska" )
    NWM_MEDIUM_RANGE_ENSEMBLE_CHANNEL_RT_ALASKA( Set.of( DataType.ENSEMBLE_FORECASTS ),
                                                 FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range deterministic channel rt alaska. */
    @JsonProperty( "nwm medium range deterministic channel rt alaska" )
    NWM_MEDIUM_RANGE_DETERMINISTIC_CHANNEL_RT_ALASKA( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                      FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm medium range no da deterministic channel rt alaska. */
    @JsonProperty( "nwm medium range no da deterministic channel rt alaska" )
    NWM_MEDIUM_RANGE_NO_DA_DETERMINISTIC_CHANNEL_RT_ALASKA( Set.of( DataType.SINGLE_VALUED_FORECASTS ),
                                                            FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim channel rt alaska. */
    @JsonProperty( "nwm analysis assim channel rt alaska" )
    NWM_ANALYSIS_ASSIM_CHANNEL_RT_ALASKA( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim no da channel rt alaska. */
    @JsonProperty( "nwm analysis assim no da channel rt alaska" )
    NWM_ANALYSIS_ASSIM_NO_DA_CHANNEL_RT_ALASKA( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim extend channel rt alaska. */
    @JsonProperty( "nwm analysis assim extend channel rt alaska" )
    NWM_ANALYSIS_ASSIM_EXTEND_CHANNEL_RT_ALASKA( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID ),
    /** nwm analysis assim extend no da channel rt alaska. */
    @JsonProperty( "nwm analysis assim extend no da channel rt alaska" )
    NWM_ANALYSIS_ASSIM_EXTEND_NO_DA_CHANNEL_RT_ALASKA( Set.of( DataType.ANALYSES ), FeatureAuthority.NWM_FEATURE_ID );

    /** The supported data types. */
    private final Set<DataType> dataTypes;

    /** The feature authority used to identify feature names in the source. */
    private final FeatureAuthority featureAuthority;

    /**
     * Creates an instance.
     * @param dataTypes the data types
     * @param featureAuthority the feature authority
     */
    SourceInterface( Set<DataType> dataTypes, FeatureAuthority featureAuthority )
    {
        this.dataTypes = dataTypes;
        this.featureAuthority = featureAuthority;
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }

    /**
     * @return the data type or null if the interface admits several types
     */
    public Set<DataType> getDataTypes()
    {
        return this.dataTypes;
    }

    /**
     * @return the feature authority.
     */
    public FeatureAuthority getFeatureAuthority()
    {
        return this.featureAuthority;
    }

    /**
     * Convenience method that inspects the interface {@link #name()} and returns <code>true</code> when the name
     * begins with 'NWM_', otherwise <code>false</code>.
     *
     * @return whether the source interface is an NWM interface
     */

    public boolean isNwmInterface()
    {
        return this.name()
                   .startsWith( "NWM_" );
    }

    /**
     * Convenience method that inspects the interface {@link #name()} and returns <code>true</code> when the name
     * contains 'ANALYSIS_', otherwise <code>false</code>.
     *
     * @return whether the source interface is an analysis interface
     */

    public boolean isAnalysisInterface()
    {
        return this.name()
                   .contains( "ANALYSIS_" );
    }
}
