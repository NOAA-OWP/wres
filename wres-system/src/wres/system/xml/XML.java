package wres.system.xml;

import java.util.List;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Christopher Tubbs
 *
 */
public final class XML
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
     * Determines if the xml tag is closed and is one of n possible tag names
     * 
     * @param reader The reader for the XML data
     * @param tagNames A list of names to check against
     * @return Returns true if the current tag is a closed tag with one of the possible names
     */
    public static boolean xmlTagClosed(XMLStreamReader reader, List<String> tagNames) {
        return reader.isEndElement() && Collections.exists(tagNames, (String name) -> {
           return name.equalsIgnoreCase(reader.getLocalName()); 
        });
    }
    
    public static boolean xmlTagClosed(XMLStreamReader reader, String tagName) {
        return reader.isEndElement() && reader.getLocalName().equalsIgnoreCase(tagName);
    }
    
    public static void skipToEndTag(XMLStreamReader reader, List<String> tagNames) throws XMLStreamException {
        while (reader.hasNext() && !xmlTagClosed(reader, tagNames)) {
            reader.next();
        }
    }
    
    public static void skipToEndTag(XMLStreamReader reader, String tagName) throws XMLStreamException {
        while (reader.hasNext() && !xmlTagClosed(reader, tagName)) {
            reader.next();
        }
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
    
    /**
     * Checks if the tag for the current element is equivalent to the one passed in
     * @param reader The point in the XML document representing the tag
     * @param tagNames Names of the tag that we are interested in
     * @return True if, ignoring case, the tag on the current element is the one that we're interested in
     */
    public static boolean tagIs(XMLStreamReader reader, String... tagNames) {
        boolean currentTagHasName = false;

        for (String tagName : tagNames) {
            currentTagHasName = reader.hasName() && reader.getLocalName().equalsIgnoreCase(tagName);
            if (currentTagHasName) {
                break;
            }
        }
        return currentTagHasName;
    }
}
