package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OriginalThresholdValues implements Serializable {
    /**
     * The measurement for low flow rate
     */
    String low_flow;

    /**
     * The measurement for bankfull flow rate
     */
    String bankfull_flow;

    /**
     * The measurement for action flow rate
     */
    String action_flow;

    /**
     * The measurement for minor flow rate
     */
    String minor_flow;

    /**
     * The measurement for moderate flow rate
     */
    String moderate_flow;

    /**
     * The measurement for major flow rate
     */
    String major_flow;

    /**
     * The measurement for record flow rate
     */
    String record_flow;
    

    /**
     * The measurement for low stage height
     */
    String low_stage;

    /**
     * The measurement for bankfull stage height
     */
    String bankfull_stage;

    /**
     * The measurement for action stage height
     */
    String action_stage;

    /**
     * The measurement for the height of the minor stage
     */
    String minor_stage;

    /**
     * The measurement for the height of the moderate stage
     */
    String moderate_stage;

    /**
     * The measurement for the height of the major stage
     */
    String major_stage;

    /**
     * The measurement for the height of the record stage
     */
    String record_stage;

    /**
     * Gets the actual low flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getLow_flow() {
        if (this.low_flow == null || this.low_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(low_flow);
    }

    /**
     * Sets the low flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param low_flow The rate for low flow conditions
     */
    public void setLow_flow(String low_flow) {
        this.low_flow = low_flow;
    }

    /**
     * Gets the actual bankfull flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getBankfull_flow() {
        if (this.bankfull_flow == null || this.bankfull_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.bankfull_flow);
    }

    /**
     * Sets the bankfull flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param bankfull_flow The rate for bankfull flow conditions
     */
    public void setBankfull_flow(String bankfull_flow) {
        this.bankfull_flow = bankfull_flow;
    }

    /**
     * Gets the actual action flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getAction_flow() {
        if (this.action_flow == null || this.action_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.action_flow);
    }

    /**
     * Sets the action flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param action_flow The rate for action flow conditions
     */
    public void setAction_flow(String action_flow) {
        this.action_flow = action_flow;
    }

    /**
     * Gets the actual minor flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMinor_flow() {
        if (this.minor_flow == null || this.minor_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.minor_flow);
    }

    /**
     * Sets the minor flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param minor_flow The rate for minor flow conditions
     */
    public void setMinor_flow(String minor_flow) {
        this.minor_flow = minor_flow;
    }

    /**
     * Gets the actual moderate flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getModerate_flow() {
        if (this.moderate_flow == null || this.moderate_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.moderate_flow);
    }

    /**
     * Sets the moderate flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param moderate_flow The rate for moderate flow conditions
     */
    public void setModerate_flow(String moderate_flow) {
        this.moderate_flow = moderate_flow;
    }

    /**
     * Gets the actual major flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMajor_flow() {
        if (this.major_flow == null || this.major_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.major_flow);
    }

    /**
     * Sets the major flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param major_flow The rate for major flow conditions
     */
    public void setMajor_flow(String major_flow) {
        this.major_flow = major_flow;
    }

    /**
     * Gets the actual record flow rate value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getRecord_flow() {
        if (this.record_flow == null || this.record_flow.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.record_flow);
    }

    /**
     * Sets the record flow rate value
     * <br>
     * <b>NOTE:</b> Used for deserialization
     *
     * @param record_flow The rate for record flow conditions
     */
    public void setRecord_flow(String record_flow) {
        this.record_flow = record_flow;
    }

    /**
     * Gets the actual low stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getLow_stage() {
        if (this.low_stage == null || this.low_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.low_stage);
    }


    /**
     * Sets the value for low stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param low_stage The height for low stage conditions
     */
    public void setLow_stage(String low_stage) {
        this.low_stage = low_stage;
    }

    /**
     * Gets the actual bankfull stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getBankfull_stage() {
        if (this.bankfull_stage == null || this.bankfull_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.bankfull_stage);
    }

    /**
     * Sets the value for bankfull stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param bankfull_stage The height for low stage conditions
     */
    public void setBankfull_stage(String bankfull_stage) {
        this.bankfull_stage = bankfull_stage;
    }

    /**
     * Gets the actual action stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getAction_stage() {
        if (this.action_stage == null || this.action_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.action_stage);
    }

    /**
     * Sets the value for action stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param action_stage The height for action stage conditions
     */
    public void setAction_stage(String action_stage) {
        this.action_stage = action_stage;
    }

    /**
     * Gets the actual minor stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMinor_stage() {
        if (this.minor_stage == null || this.minor_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.minor_stage);
    }

    /**
     * Sets the value for minor stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param minor_stage The height for minor stage conditions
     */
    public void setMinor_stage(String minor_stage) {
        this.minor_stage = minor_stage;
    }

    /**
     * Gets the actual moderate stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getModerate_stage() {
        if (this.moderate_stage == null || this.moderate_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.moderate_stage);
    }

    /**
     * Sets the value for moderate stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param moderate_stage The height for moderate stage conditions
     */
    public void setModerate_stage(String moderate_stage) {
        this.moderate_stage = moderate_stage;
    }

    /**
     * Gets the actual major stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getMajor_stage() {
        if (this.major_stage == null || this.major_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.major_stage);
    }

    /**
     * Sets the value for major stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param major_stage The height for minor stage conditions
     */
    public void setMajor_stage(String major_stage) {
        this.major_stage = major_stage;
    }

    /**
     * Gets the actual record stage height value
     *
     * @return null if a real value wasn't retrieved, the parsed value otherwise
     */
    public Double getRecord_stage() {
        if (this.record_stage == null || this.record_stage.toLowerCase().equals("none")) {
            return null;
        }
        return Double.parseDouble(this.record_stage);
    }

    /**
     * Sets the value for record stage height
     * <br>
     *     <b>NOTE:</b> Used for deserialization
     *
     * @param record_stage The height for record stage conditions
     */
    public void setRecord_stage(String record_stage) {
        this.record_stage = record_stage;
    }
}
