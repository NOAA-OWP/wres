package gov.noaa.wres.pixml;

import javax.xml.stream.XMLStreamReader;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.JAXBException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import java.time.LocalDateTime;
import java.time.ZoneId;

import gov.noaa.wres.io.XmlReaderInfo;
import gov.noaa.wres.pixml.Header;
import gov.noaa.wres.datamodel.SeriesInfo;

public class ReadPiIntoHeader
    implements Function<XmlReaderInfo,SeriesInfo>
{
    public SeriesInfo apply(XmlReaderInfo xri)
    {
        XMLStreamReader reader = xri.getXMLStreamReader();
        Unmarshaller unmarsh = xri.getUnmarshaller();
        try
        {
            Header xmlHeader = (Header) unmarsh.unmarshal(reader);
            // make a seriesinfo
            SeriesInfo seriesInfo = null;
            if (xmlHeader != null)
            {
                LocalDateTime startTime = null;
                if (xmlHeader.startDate != null)
                {
                    startTime = LocalDateTime.of(xmlHeader.startDate.date,
                                                 xmlHeader.startDate.time);
                }

                ZoneId zone = null;
                // make a Map.
                Map<String,Object> metadata = null;
                    
                metadata = new ConcurrentHashMap<>();
                metadata.put("missingValue", xmlHeader.missVal);
                metadata.put("locationId", xmlHeader.locationId);
                metadata.put("unitOfMeasure", xmlHeader.units);
                metadata.put("variableName", xmlHeader.parameterId);
                
                // now that we have everything, make the object.
                seriesInfo = SeriesInfo.of(startTime, zone, metadata);
            }
            return seriesInfo;
        }
        catch (JAXBException e)
        {
            System.err.println("Stinking jaxb exception" + e);
            e.printStackTrace();
        }
        return null;
    }
}
