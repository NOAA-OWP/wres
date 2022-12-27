package wres.io.reading;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.junit.jupiter.api.Test;

import jakarta.xml.bind.DatatypeConverter;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Tests {@link DataSource}.
 * 
 * @author James Brown
 */

class DataSourceTest
{
    @Test
    void testDetectFormatIdentifiesOneColumnDatacardWithComments() throws IOException
    {
        String formatString = "$  IDENTIFIER=               DESCRIPTION=INFLOW\r\n"
                              + "$  PERIOD OF RECORD=10/1969 THRU 05/2014\r\n"
                              + "$  SYMBOL FOR MISSING DATA=-999.00   SYMBOL FOR ACCUMULATED DATA=-998.00\r\n"
                              + "$ YUBA\r\n"
                              + "$ NEW BULLARDS BAR            Regulated inflow data is a merged data set.  Regulated inflow from WY 1970-2004 comes from USGS daily data (11413520+11413515+11413510).\r\n"
                              + "$ REGULATED INFLOW            Regulated inflow from WY2004-2014 came from USACE-SPK which got their data from YCWA.\r\n"
                              + "$                    Some of the inflow \"noise\" was smoothed for the 1970-2004 period.\r\n"
                              + "$ WHITIN MERGE WITH USACE\r\n"
                              + "DATACARD      AQME L3   CFSD 24                  INFLOW\r\n"
                              + "10  1969  5   2014  1   F10.2\r\n"
                              + "            1069  01    135.50";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.datacard" );

            assertEquals( DataDisposition.DATACARD, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesOneColumnDatacardWithoutComments() throws IOException
    {
        String formatString = "datacard      QME  L3   CFSD 24   11473900        MF EEL R NR DOS RIO\r\n"
                              + "10  1948 09   2011  1   f10.2\r\n"
                              + "11473900    1048   1   -999.00\r\n"
                              + "11473900    1048   2   -999.00";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.datacard" );

            assertEquals( DataDisposition.DATACARD, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesFourColumnDatacardWithCommentsAndUnixNewLines() throws IOException
    {
        String formatString = "$ stored in the prodly/caldly database table.  id = DOLC2  pc = QRD5ZZZ\n"
                              + "DATACARD      QME  L3   CFSD 24\n"
                              + " 1  1951  9   2006  4   F15.6\n"
                              + "DOLC2        151   1      42.000000      39.000000      37.000000      39.000000\n"
                              + "DOLC2        151   2      41.000000      37.000000      33.000000      36.000000";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.datacard" );

            assertEquals( DataDisposition.DATACARD, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesSixColumnDatacardWithCommentsAndUnixNewLines() throws IOException
    {
        String formatString = "$  IDENTIFIER=1620           DESCRIPTION=1620                \n"
                              + "$  PERIOD OF RECORD=01/1948 THRU 09/2013\n"
                              + "$  SYMBOL FOR MISSING DATA=-999.00   SYMBOL FOR ACCUMULATED DATA=-998.00\n"
                              + "$  TYPE=MAT    UNITS=   F   DIMENSIONS=TEMP   DATA TIME INTERVAL= 6 HOURS\n"
                              + "$  OUTPUT FORMAT=(3A4,2I2,I4,6F9.3)              \n"
                              + "DATACARD      MAT  TEMP    F  6   1620           1620                \n"
                              + " 1  1948  9   2013  6   F9.3        \n"
                              + "             148   1   12.111   18.084   21.048    7.573    1.365   18.053\n"
                              + "             148   2   28.468   20.433   16.470   31.302   40.343   32.673";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.datacard" );

            assertEquals( DataDisposition.DATACARD, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesPublishedInterfaceXmlWithUnixNewLines() throws IOException
    {
        String formatString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                              + "<TimeSeries xmlns=\"http://www.wldelft.nl/fews/PI\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd\" version=\"1.14\">\n"
                              + "  <timeZone>0.0</timeZone>\n"
                              + "  <series>\n"
                              + "    <header>\n"
                              + "      <type>instantaneous</type>\n"
                              + "      <locationId>CKLN6</locationId>\n"
                              + "      <parameterId>STG</parameterId>\n"
                              + "       <timeStep unit=\"second\" multiplier=\"900\"/>\n"
                              + "      <startDate date=\"2017-01-01\" time=\"00:00:00\"/>\n"
                              + "      <endDate date=\"2017-12-31\" time=\"00:00:00\"/>\n"
                              + "      <missVal>-9999.0</missVal>\n"
                              + "      <units>FT</units>\n"
                              + "    </header>\n"
                              + "    <event date=\"2017-01-01\" time=\"00:00:00\" value=\"5.07\" flag=\"0\"/>\n"
                              + "    <event date=\"2017-01-01\" time=\"00:15:00\" value=\"5.06\" flag=\"0\"/>";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.xml" );

            assertEquals( DataDisposition.XML_PI_TIMESERIES, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesWresCsv() throws IOException
    {
        String formatString = "value_date,variable_name,location,measurement_unit,value\r\n"
                              + "1985-06-01T13:00:00Z,QINE,DRRC2,CFS,747.78455\r\n"
                              + "1985-06-01T14:00:00Z,QINE,DRRC2,CFS,735.21606\r\n"
                              + "1985-06-01T15:00:00Z,QINE,DRRC2,CFS,722.6476";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.csv" );

            assertEquals( DataDisposition.CSV_WRES, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesJsonWrdsAhps() throws IOException
    {
        String formatString = "{\r\n"
                              + "  \"header\": {\r\n"
                              + "    \"request\": {\r\n"
                              + "      \"url\": \" http://fake.url\",\r\n"
                              + "      \"path\": \"/fake_bucket\"\r\n"
                              + "    }\r\n"
                              + "  },\r\n"
                              + "  \"status_code\": 200,\r\n"
                              + "  \"messsage\": \"\",\r\n"
                              + "  \"forecasts\": [\r\n"
                              + "    {\r\n"
                              + "      \"location\": {\r\n"
                              + "        \"names\": {\r\n"
                              + "          \"nwsLid\": \"FAKE2\",\r\n"
                              + "          \"usgsSiteCode\": \"-123456789\",\r\n"
                              + "          \"comId\": \"-987654321\",\r\n"
                              + "          \"nwsName\": \"FAKE2 - FAKETY, FAKE\"\r\n"
                              + "        }\r\n"
                              + "      },";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.json" );

            assertEquals( DataDisposition.JSON_WRDS_AHPS, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesJsonWrdsNwm() throws IOException
    {
        String formatString = "{\r\n"
                              + "    \"_metrics\": {\r\n"
                              + "        \"location_api_call\": 17.621362447738647,\r\n"
                              + "        \"forming_location_data\": 0.0001201629638671875,\r\n"
                              + "        \"usgs_short_range_reference_time_count\": 257,\r\n"
                              + "        \"short_range_reference_time_count\": 257,\r\n"
                              + "        \"validate_nwm_vars\": 0.04367184638977051,\r\n"
                              + "        \"short_range_feature_id_count\": 1,\r\n"
                              + "        \"scale_factor\": 0.009999999776482582,\r\n"
                              + "        \"total_request_time\": 22.629672050476074\r\n"
                              + "    },\r\n"
                              + "    \"_documentation\": \"https://***REMOVED***.***REMOVED***.***REMOVED***/docs/nwm2.1/v2.0/swagger/";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.json" );

            assertEquals( DataDisposition.JSON_WRDS_NWM, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesJsonWaterMl() throws IOException
    {
        String formatString =
                "{\"name\":\"ns1:timeSeriesResponseType\",\"declaredType\":\"org.cuahsi.waterml.TimeSeriesResponseType\",\"scope\"";

        try ( InputStream stream = new ByteArrayInputStream( formatString.getBytes() ) )
        {
            URI fakeUri = URI.create( "fake.json" );

            assertEquals( DataDisposition.JSON_WATERML, DataSource.detectFormat( stream, fakeUri ) );
        }
    }

    @Test
    void testDetectFormatIdentifiesFastInfosetPublishedInterfaceXml() throws IOException
    {
        // A FastInfoset hex string, converted to bytes
        byte[] bytes =
                DatatypeConverter.parseHexBinary( "e00000010078cd1c687474703a2f2f7777772e776c64656c66742e6e6c2f666577732f5049f03d810954696d655365726965737808786d6c6e733a7873690820687474703a2f2f7777772e77332e6f72672f323030312f584d4c536368656d612d696e7374616e636578117873693a736368656d614c6f636174696f6e085b687474703a2f2f7777772e776c64656c66742e6e6c2f666577732f504920687474703a2f2f666577732e776c64656c66742e6e6c2f736368656d61732f76657273696f6e312e302f70692d736368656d61732f70695f74696d657365726965732e787364780676657273696f6e42312e39f03d810774696d655a6f6e659200302e30f03d81057365726965733d81056865616465723d810374797065920a696e7374616e74616e656f7573f03d81096c6f636174696f6e496492024352454331f03d810a706172616d65746572496492015351494ef03d8109656e73656d626c65496492014d454650f03d8112656e73656d626c654d656d626572496e646578920131393530f07d810774696d65537465707803756e6974457365636f6e6478096d756c7469706c6965724333363030ff7d81087374617274446174657803646174654801323031302d30382d3234780374696d654731323a30303a3030ff7d8106656e6444617465054801323031302d30392d30370684ff7d810b666f7265636173744461746505830684ff3d81066d69737356616c92004e614ef03d810a73746174696f6e4e616d659216534d495448202d204352455343454e5420434954592c204e52f03d81026c6174920e34312e3738393434333936393732363536f03d81026c6f6e92102d3132342e3035333838363431333537343232f03d810078a9f03d810079a8f03d81007a920e33332e3532373939393837373932393639f03d8104756e6974739200434d53f03d810e66696c654465736372697074696f6e920472656d6f766564f03d810b6372656174696f6e446174659207323032312d30362d3033f03d810b6372656174696f6e54696d65920532333a35333a3233ff7d81046576656e7405830684780476616c7565480031322e3334373237387803666c61674030ff580583064731333a30303a303007480031322e3333393036360887ff580583064731343a30303a3030074731322e33333035370887ff580583064731353a30303a303007480031322e3332323634310887ff580583064731363a30303a3030074731322e33313434330887ff580583064731373a30303a303007480031322e3330363231380887ff580583064731383a30303a303007480031322e3239373732330887ff580583064731393a30303a303007480031322e3238393531310887ff580583064732303a30303a303007480031322e323831353832" );

        try ( InputStream stream = new ByteArrayInputStream( bytes ) )
        {
            URI fakeUri = URI.create( "fake.fi" );

            assertEquals( DataDisposition.XML_FI_TIMESERIES, DataSource.detectFormat( stream, fakeUri ) );
        }
    }
}
