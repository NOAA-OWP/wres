package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class GeneralThresholdResponse implements Serializable {

    Collection<GeneralThresholdDefinition> value_set;

    /**
     * Convenience wrapper with more intuitive name.
     * @return Calls {@link #getValue_set()} and returns results.
     */
    public Collection<GeneralThresholdDefinition> getThresholds() {
        return getValue_set();
    }

    /**
     * Convenience wrapper with more intuitive name.  Calls {@link #setValue_set(Collection)}.
     * @param value_set the value set
     */
    public void setThresholds(Collection<GeneralThresholdDefinition> value_set) {
        setValue_set(value_set);
    }

    public Collection<GeneralThresholdDefinition> getValue_set()
    {
        return value_set;
    }

    public void setValue_set( Collection<GeneralThresholdDefinition> value_set )
    {
        this.value_set = value_set;
    } 
}
