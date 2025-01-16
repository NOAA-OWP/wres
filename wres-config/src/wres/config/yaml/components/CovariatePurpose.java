package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The purpose or application of a covariate dataset within an evaluation.
 * @author James Brown
 */
public enum CovariatePurpose
{
    /** Use the covariate dataset to filter the pairs. */
    @JsonProperty( "filter" ) FILTER,
    /** Use the covariate dataset to detect events for evaluation. */
    @JsonProperty( "detect" ) DETECT
}