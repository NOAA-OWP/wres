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
    @JsonProperty( "observations" ) OBSERVATIONS( false, true ),
    /** Analyses. These are not observation like because they may be generated at different cycles. */
    @JsonProperty( "analyses" ) ANALYSES( false, false ),
    /** Single-valued forecasts. */
    @JsonProperty( "single valued forecasts" ) SINGLE_VALUED_FORECASTS( true, false ),
    /** Ensemble forecasts. */
    @JsonProperty( "ensemble forecasts" ) ENSEMBLE_FORECASTS( true, false ),
    /** Simulations, which have the same time-series data structure as {@link #OBSERVATIONS}. For this reason, it has
     * no distinct role, but is included to help a user choose from a list of options. */
    @JsonProperty( "simulations" ) SIMULATIONS( false, true );

    /** Is the dataset a forecast type? **/
    private final boolean isForecastType;

    /** Is the dataset observation-like? **/
    private final boolean isObservationLike;

    /**
     * @return whether the data type is a forecast type
     */
    public boolean isForecastType()
    {
        return this.isForecastType;
    }

    /**
     * @return whether the data type is observation-like
     */
    public boolean isObservationLike()
    {
        return this.isObservationLike;
    }

    /**
     * @param isForecastType whether the data type is a forecast
     * @param isObservationLike whether the data type is observation-like
     */
    DataType( boolean isForecastType, boolean isObservationLike )
    {
        this.isForecastType = isForecastType;
        this.isObservationLike = isObservationLike;
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
