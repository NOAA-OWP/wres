/**
 * 
 */
package config.data;

import java.util.List;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class FeatureSelector extends ClauseConfig
{

    /**
     * @param reader
     */
    public FeatureSelector(XMLStreamReader reader)
    {
        super(reader);
    }

    public abstract List<Integer> getVariablePositionIDs(Integer variableID) throws Exception;

}
