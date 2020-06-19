package wres.io.thresholds.wrds.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class ThresholdResponse implements Serializable {
    Map<String, Double> _metrics;
    String _documentation;
    Collection<ThresholdDefinition> thresholds;

    public Map<String, Double> get_metrics() {
        return _metrics;
    }

    public void set_metrics(Map<String, Double> _metrics) {
        this._metrics = _metrics;
    }

    public String get_documentation() {
        return _documentation;
    }

    public void set_documentation(String _documentation) {
        this._documentation = _documentation;
    }

    public Collection<ThresholdDefinition> getThresholds() {
        return thresholds;
    }

    public void setThresholds(Collection<ThresholdDefinition> thresholds) {
        this.thresholds = thresholds;
    }
}
