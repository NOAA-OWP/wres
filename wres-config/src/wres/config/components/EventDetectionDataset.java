package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The context of a dataset to use for event detection.
 * @author James Brown
 */
public enum EventDetectionDataset
{
    /** Observed dataset. */
    @JsonProperty( "observed" ) OBSERVED,
    /** Predicted dataset. */
    @JsonProperty( "predicted" ) PREDICTED,
    /** Baseline dataset. */
    @JsonProperty( "baseline" ) BASELINE,
    /** Covariates dataset. */
    @JsonProperty( "covariates" ) COVARIATES
}