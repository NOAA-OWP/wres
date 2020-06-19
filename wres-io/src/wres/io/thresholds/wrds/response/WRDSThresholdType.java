package wres.io.thresholds.wrds.response;

/**
 * The WRDS variable that a threshold applies to
 */
public enum WRDSThresholdType {
    /**
     * Applies to streamflow data
     */
    FLOW,
    /**
     * Applies to stage data
     */
    STAGE
}
