package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * The type or structure of the time-series data to evaluate.
 * @author James Brown
 */
public enum DataType
{
    /** Observations. */
    @JsonProperty( "observations" ) OBSERVATIONS( false ),
    /** Analyses. */
    @JsonProperty( "analyses" ) ANALYSES( false ),
    /** Single-valued forecasts. */
    @JsonProperty( "single valued forecasts" ) SINGLE_VALUED_FORECASTS( true ),
    /** Ensemble forecasts. */
    @JsonProperty( "ensemble forecasts" ) ENSEMBLE_FORECASTS( true ),
    /** Simulations, which have the same time-series data structure as {@link #OBSERVATIONS}. For this reason, it has
     * no distinct role, but is included to help a user choose from a list of options. */
    @JsonProperty( "simulations" ) SIMULATIONS( false );

    /** Is the dataset a forecast type? **/
    private final boolean isForecastType;

    /**
     * @return whether the data type is a forecast type
     */
    public boolean isForecastType()
    {
        return this.isForecastType;
    }

    /**
     * @param isForecastType whether the data type is a forecast
     */
    DataType( boolean isForecastType )
    {
        this.isForecastType = isForecastType;
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
