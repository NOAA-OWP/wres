package wres.reading.wrds.thresholds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A threshold response.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
@XmlRootElement
class ThresholdResponse implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6355701807768699282L;

    /** Value set. */
    @JsonProperty( "value_set" )
    private Collection<ThresholdDefinition> valueSet;

    /**
     * Convenience wrapper with more intuitive name.
     * @return Calls {@link #getValueSet()} and returns results.
     */
    Collection<ThresholdDefinition> getThresholds()
    {
        return getValueSet();
    }

    /**
     * Convenience wrapper with more intuitive name.  Calls {@link #setValueSet(Collection)}.
     * @param valueSet the value set
     */
    void setThresholds( Collection<ThresholdDefinition> valueSet )
    {
        setValueSet( valueSet );
    }

    /**
     * @return the value set
     */
    Collection<ThresholdDefinition> getValueSet()
    {
        return valueSet;
    }

    /**
     * Sets the value set.
     * @param valueSet the value set
     */
    void setValueSet( Collection<ThresholdDefinition> valueSet )
    {
        this.valueSet = valueSet;
    }
}
