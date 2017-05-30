/**
 * 
 */
package wres.io.config.specification;

import java.util.List;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Christopher Tubbs
 *
 */
public abstract class FeatureSpecification extends SpecificationElement
{
    public static final int LOCATION = 0;
    public static final int RANGE = 1;
    public static final int POLYGON = 2;
    public static final int POINT = 3;

    /**
     * @param reader
     */
    public FeatureSpecification(XMLStreamReader reader)
    {
        super(reader);
    }

    public abstract List<Integer> getVariablePositionIDs(Integer variableID) throws Exception;

    public abstract int getFeatureType();
    
    public boolean isLocation() {
        return this.getFeatureType() == FeatureSpecification.LOCATION;
    }
    
    public boolean isRange() {
        return this.getFeatureType() == FeatureSpecification.RANGE;
    }
    
    public boolean isPolygon() {
        return this.getFeatureType() == FeatureSpecification.POLYGON;
    }
    
    public boolean isPoint() {
        return this.getFeatureType() == FeatureSpecification.POINT;
    }
}
