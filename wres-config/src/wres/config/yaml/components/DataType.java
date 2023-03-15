package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type or structure of the time-series data to evaluate.
 * @author James Brown
 */
public enum DataType
{
    /** Observations. */
    @JsonProperty( "observations" ) OBSERVATIONS,
    /** Analyses. */
    @JsonProperty( "analyses" ) ANALYSES,
    /** Single-valued forecasts. */
    @JsonProperty( "single valued forecasts" ) SINGLE_VALUED_FORECASTS,
    /** Ensemble forecasts. */
    @JsonProperty( "ensemble forecasts" ) ENSEMBLE_FORECASTS,
    /** Simulations, which have the same time-series data structure as {@link #OBSERVATIONS}. For this reason, it has
     * no distinct role, but is included to help a user choose from a list of options. */
    @JsonProperty( "simulations" ) SIMULATIONS,
}
