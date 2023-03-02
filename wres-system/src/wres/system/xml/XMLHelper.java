package wres.system.xml;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Christopher Tubbs
 *
 * A collection of XML helper functions
 */
public final class XMLHelper
{
    /**
     * Gets the text within an xml element
     * <br>
     * <b>The stream will attempt to move forward within the source</b>
     * @param reader The reader for the XML data
     * @return The trimed text within the xml node. Null is returned if no text is found
     * @throws XMLStreamException if there is an error processing the underlying XML source
     */
    public static String getXMLText(XMLStreamReader reader) throws XMLStreamException {
        String value = null;

        if (reader.isStartElement() && (reader.next() == XMLStreamConstants.CHARACTERS)) {
            value = reader.getText().trim();
        }

        return value;
    }

    /**
     * Searches for and finds the value for the given attribute on the passed in XML node
     * @param reader The stream containing the XML data
     * @param attributeName The name of the attribute to search for
     * @return The value of the attribute on the XML node. Null is returned if the attribute isn't found.
     */
    public static String getAttributeValue(XMLStreamReader reader, String attributeName) {
        String value = null;
        
        for (int attributeIndex = 0; attributeIndex < reader.getAttributeCount(); ++attributeIndex) {
            if (reader.getAttributeLocalName(attributeIndex).equalsIgnoreCase(attributeName)) {
                value = reader.getAttributeValue(attributeIndex);
            }
        }
        
        return value;
    }
}
