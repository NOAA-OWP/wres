package wres.io.reading.wrds.thresholds.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A threshold response.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
@XmlRootElement
public class ThresholdResponse implements Serializable
{
    @Serial
    private static final long serialVersionUID = 508326690122422103L;

    /** Metrics. */
    @JsonProperty( "_metrics" )
    private Map<String, Double> metrics;
    /** Documentation. */
    @JsonProperty( "_documentation" )
    private String documentation;
    /** Thresholds. */
    private Collection<ThresholdDefinition> thresholds;

    /**
     * @return the metrics
     */
    public Map<String, Double> getMetrics()
    {
        return metrics;
    }

    /**
     * Sets the metrics.
     * @param metrics the metrics
     */
    public void setMetrics( Map<String, Double> metrics )
    {
        this.metrics = metrics;
    }

    /**
     * @return the documentation
     */
    public String getDocumentation()
    {
        return documentation;
    }

    /**
     * Sets the documentation
     * @param documentation the documentation
     */
    public void setDocumentation( String documentation )
    {
        this.documentation = documentation;
    }

    /**
     * @return the thresholds
     */
    public Collection<ThresholdDefinition> getThresholds()
    {
        return thresholds;
    }

    /**
     * Sets the thresholds.
     * @param thresholds the thresholds
     */
    public void setThresholds( Collection<ThresholdDefinition> thresholds )
    {
        this.thresholds = thresholds;
    }
}
