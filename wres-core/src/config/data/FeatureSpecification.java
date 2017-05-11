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
public abstract class FeatureSpecification extends SpecificationElement
{

    /**
     * @param reader
     */
    public FeatureSpecification(XMLStreamReader reader)
    {
        super(reader);
    }

    public abstract List<Integer> getVariablePositionIDs(Integer variableID) throws Exception;

}
