package wres.io.thresholds.wrds.v3;

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
public class GeneralThresholdResponse implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6355701807768699282L;

    /** Value set. */
    @JsonProperty( "value_set" )
    private Collection<GeneralThresholdDefinition> valueSet;

    /**
     * Convenience wrapper with more intuitive name.
     * @return Calls {@link #getValueSet()} and returns results.
     */
    public Collection<GeneralThresholdDefinition> getThresholds()
    {
        return getValueSet();
    }

    /**
     * Convenience wrapper with more intuitive name.  Calls {@link #setValueSet(Collection)}.
     * @param valueSet the value set
     */
    public void setThresholds( Collection<GeneralThresholdDefinition> valueSet )
    {
        setValueSet( valueSet );
    }

    /**
     * @return the value set
     */
    public Collection<GeneralThresholdDefinition> getValueSet()
    {
        return valueSet;
    }

    /**
     * Sets the value set.
     * @param valueSet the value set
     */
    public void setValueSet( Collection<GeneralThresholdDefinition> valueSet )
    {
        this.valueSet = valueSet;
    }
}
