package wres.io.reading;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.fastinfoset.stax.StAXDocumentParser; //NOSONAR

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;

/**
 * A data source.
 */

public class DataSource
{
    private static final String WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE =
            "While checking {} for a Datacard string, discovered that the second non-comment line ";

    private static final Logger LOGGER = LoggerFactory.getLogger( DataSource.class );

    /**
     * The disposition of the original or generated-from-original source
     * declaration. This is for software to determine, not the user.
     */

    public enum DataDisposition
    {
        /** The data has been detected as a gzip stream. */
        GZIP,
        /** The data has been detected as a tarball. */
        TARBALL,
        /** The data has been detected as a pi-xml timeseries stream. */
        XML_PI_TIMESERIES,
        /** The data has been detected as a fast-infoset encoded pi-xml 
         * timeseries stream. */
        XML_FI_TIMESERIES,
        /** The data has been detected as a datacard stream. */
        DATACARD,
        /** The data has been detected as a netCDF blob with gridded data. */
        NETCDF_GRIDDED,
        /** The data has been detected as a netCDF blob with vector data. */
        NETCDF_VECTOR,
        /** The data has been detected as a json, wrds/nwm stream. */
        JSON_WRDS_NWM,
        /** The data has been detected as a json, wrds/ahps stream. */
        JSON_WRDS_AHPS,
        /** The data has been detected as a json, waterml stream. */
        JSON_WATERML,
        /** The data has been detected as a csv, wres stream. */
        CSV_WRES,
        /** The data type is unknown or to be determined. */
        UNKNOWN;

        @Override
        public String toString()
        {
            return this.name();
        }
    }

    /**
     * The count of bytes to use for detecting WRES-compatible type.
     */

    private static final int DETECTION_BYTES = 1024;

    /**
     * The disposition of the data for this source.
     */

    private final DataDisposition disposition;

    /**
     * The context in which this source is declared.
     */

    private final DataSourceConfig context;

    /**
     * The source to load and link.
     */

    private final DataSourceConfig.Source source;

    /**
     * Additional links; may be empty, in which case, link only to
     * its own {@link #context}.
     */

    private final List<LeftOrRightOrBaseline> links;

    /**
     * URI of the source. Required when ingesting, but null when this object is
     * used in {@link IngestResult} to report back that ingest is complete, for
     * heap savings.
     */

    private final URI uri;

    /**
     * Whether the source originates from the left, right or baseline side of the evaluation.
     */

    private final LeftOrRightOrBaseline lrb;

    /**
     * Create a data source to load into <code>wres.Source</code>, with optional links to
     * create in <code>wres.ProjectSource</code>. If the source is used only once in the
     * declaration, there will be no additional links and the set of links should be empty.
     * The evaluated path to the source may not match the URI within the source, because
     * the path has been evaluated. For example, evaluation means to decompose a
     * source directory into separate paths to each file that must be loaded. Each
     * file has a separate {@link DataSource}. For each of those decomposed paths, there
     * is only one {@link DataSourceConfig.Source}.
     *
     * @param disposition the disposition of the data source or data inside
     * @param source the source to load
     * @param context the context in which the source appears
     * @param links the optional links to create
     * @param uri the uri for the source
     * @param lrb whether the data source originates from the left or right or baseline side of the evaluation
     * @throws NullPointerException if any input is null
     * @return The newly created DataSource.
     */

    public static DataSource of( DataDisposition disposition,
                                 DataSourceConfig.Source source,
                                 DataSourceConfig context,
                                 List<LeftOrRightOrBaseline> links,
                                 URI uri,
                                 LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( disposition );
        Objects.requireNonNull( uri );
        return new DataSource( disposition,
                               source,
                               context,
                               links,
                               uri,
                               lrb );
    }

    /**
     * Create a source.
     * @param disposition the disposition of the data source or data inside
     * @param source the source
     * @param context the context in which the source appears
     * @param links the links
     * @param uri the uri
     * @param lrb whether the data source originates from the left or right or baseline side of the evaluation
     */

    private DataSource( DataDisposition disposition,
                        DataSourceConfig.Source source,
                        DataSourceConfig context,
                        List<LeftOrRightOrBaseline> links,
                        URI uri,
                        LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( disposition );
        Objects.requireNonNull( context );
        Objects.requireNonNull( links );

        this.disposition = disposition;
        this.source = source;
        this.context = context;

        if ( links.equals( Collections.emptyList() ) )
        {
            this.links = links;
        }
        else
        {
            this.links = Collections.unmodifiableList( links );
        }

        this.uri = uri;
        this.lrb = lrb;
    }

    /**
     * The disposition of this data source. Used to determine the reader for
     * simple sources or whether more decomposition or recomposition is needed.
     * @return The disposition.
     */

    public DataDisposition getDisposition()
    {
        return this.disposition;
    }

    /**
     * Returns the type of link to create.
     *
     * @return the type of link
     */

    public List<LeftOrRightOrBaseline> getLinks()
    {
        // Rendered immutable on construction
        return this.links;
    }

    /**
     * Returns the data source.
     *
     * @return the source
     */

    public DataSourceConfig.Source getSource()
    {
        return this.source;
    }

    /**
     * Returns the data source path.
     *
     * @return the path
     */

    public URI getUri()
    {
        return this.uri;
    }

    /**
     * Returns <code>true</code> if the data source path is not null, otherwise
     * <code>false</code>. The path may not be available for some services,
     * for which the system knows the path.
     *
     * @return true if the path is available, otherwise false
     */

    public boolean hasSourcePath()
    {
        return Objects.nonNull( this.getUri() );
    }

    /**
     * Returns the context in which the source appears.
     *
     * @return the context
     */

    public DataSourceConfig getContext()
    {
        return this.context;
    }

    /**
     * @return whether the data source originates from the left, right or baseline side of the evaluation
     */

    public LeftOrRightOrBaseline getLeftOrRightOrBaseline()
    {
        return lrb;
    }

    /**
     * Returns the variable specified for this source, null if unspecified
     * @return the variable
     */
    public DataSourceConfig.Variable getVariable()
    {
        return this.getContext()
                   .getVariable();
    }

    /**
     * @return whether this data source contains a gridded dataset, which requires special treatment in some contexts
     */

    public boolean isGridded()
    {
        return this.getDisposition() == DataDisposition.NETCDF_GRIDDED;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DataSource that = ( DataSource ) o;
        return source.equals( that.source ) &&
               links.equals( that.links )
               &&
               Objects.equals( context, that.context )
               &&
               Objects.equals( uri, that.uri );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.context,
                             this.source,
                             this.links,
                             this.uri );
    }

    @Override
    public String toString()
    {
        // Improved for #63493

        StringJoiner joiner = new StringJoiner( ";", "(", ")" );

        joiner.add( "Disposition: " + this.getDisposition() );
        joiner.add( " URI: " + this.getUri() );
        joiner.add( " Type: " + this.getContext().getType() );
        joiner.add( " Orientation: " + this.getLeftOrRightOrBaseline() );

        if ( !this.getLinks().isEmpty() )
        {
            joiner.add( " Links to other contexts: " + this.getLinks() );
        }

        return joiner.toString();
    }


    /**
     * Look at the data, open the data, to detect its WRES type.
     * @param uri The uri to examine.
     * @return The WRES Format guess, or UNKNOWN if unknown.
     * @throws PreIngestException When format detection encounters an exception.
     */

    public static DataDisposition detectFormat( URI uri )
    {
        LOGGER.debug( "Starting to detectFormat on {}", uri );

        try ( InputStream stream = TikaInputStream.get( uri ) )
        {
            DataDisposition disposition = DataSource.detectFormat( stream, uri );
            LOGGER.debug( "Finished detectFormat on {}", uri );
            return disposition;
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to open '" + uri + "'", ioe );
        }
    }

    /**
     * Look at the data, open the data to detect its WRES type.
     *
     * @param inputStream The stream to examine, at position 0, will be kept
     *                    there, must support mark/reset.
     * @param uri A real or fake URI (basically a name) to log with messages.
     * @return The WRES guess, or UNKNOWN if unknown.
     * @throws PreIngestException When format detection encounters an exception.
     * @throws UnsupportedOperationException When !inputStream.markSupported()
     */

    public static DataDisposition detectFormat( InputStream inputStream, URI uri )
    {
        LOGGER.debug( "detectFormat called on input stream for {}", uri );

        // Determine the media type
        Pair<MediaType, byte[]> mediaTypeAndBytes = DataSource.getMediaType( inputStream, uri );

        MediaType detectedMediaType = mediaTypeAndBytes.getLeft();
        byte[] firstBytes = mediaTypeAndBytes.getRight();

        String mediaType = detectedMediaType.getType()
                                            .toLowerCase();
        String subtype = detectedMediaType.getSubtype()
                                          .toLowerCase();
        LOGGER.debug( "For data labeled {}, mediaType={}, subtype={}",
                      uri,
                      mediaType,
                      subtype );

        DataDisposition disposition = DataDisposition.UNKNOWN;

        if ( firstBytes.length < 4 )
        {
            LOGGER.warn( "Found document with only {} bytes: '{}'",
                         firstBytes.length,
                         uri );
        }
        else if ( subtype.equals( "xml" ) )
        {
            disposition = DataSource.getDispositionOfXmlSubtype( firstBytes, uri );
        }
        else if ( subtype.equals( "json" ) && firstBytes.length > 4 )
        {
            disposition = DataSource.getDispositionOfJsonSubtype( firstBytes, uri );
        }
        else if ( subtype.equals( "csv" ) )
        {
            disposition = DataSource.getDispositionOfCsvSubtype( firstBytes, uri );
        }
        else if ( subtype.equals( "x-hdf" ) )
        {
            // Not much more can be done accurately from here for netCDF short
            // of opening up the file using netCDF libraries, which is the job
            // of the reader.
            // The caller will probably want to use a pattern to narrow it down,
            // or the reader will need to be lenient.
            disposition = DataDisposition.NETCDF_GRIDDED;
        }
        else if ( subtype.equals( "gzip" ) )
        {
            disposition = DataDisposition.GZIP;
        }
        else if ( subtype.equals( "x-tar" ) || subtype.equals( "x-gtar" ) )
        {
            disposition = DataDisposition.TARBALL;
        }
        else if ( subtype.equals( "plain" ) && mediaType.equals( "text" ) )
        {
            disposition = DataSource.getDispositionOfTextTypeAndPlainSubtype( firstBytes, uri );
        }
        else if ( subtype.equals( "fastinfosetxml" ) )
        {
            disposition = DataSource.getDispositionOfFastInfosetSubtype( firstBytes, uri );
        }

        LOGGER.debug( "Detected {} for {}", disposition, uri );
        return disposition;
    }

    /**
     * Determines the media type.
     *
     * @param inputStream The stream to examine, at position 0, will be kept
     *                    there, must support mark/reset.
     * @param uri A real or fake URI (basically a name) to log with messages.
     * @return the detected media type and first 4096 bytes
     */

    public static Pair<MediaType, byte[]> getMediaType( InputStream inputStream, URI uri )
    {
        LOGGER.debug( "detectFormat called on input stream for {}", uri );
        MediaType detectedMediaType;
        Metadata metadata = new Metadata();
        metadata.set( TikaCoreProperties.RESOURCE_NAME_KEY,
                      uri.toString() );
        byte[] firstBytes;

        try
        {
            // Do content type detection with tika and get the first 1024 bytes.
            TikaConfig tikaConfig = new TikaConfig();
            Detector detector = tikaConfig.getDetector();
            detectedMediaType = detector.detect( inputStream, metadata );

            if ( inputStream.markSupported() )
            {
                inputStream.mark( 4096 );
            }
            else
            {
                throw new UnsupportedOperationException( "Mark not supported!"
                                                         + " E.g. send a "
                                                         + "BufferedInputStream "
                                                         + "or TikaInputStream "
                                                         + "instead." );
            }

            firstBytes = inputStream.readNBytes( DETECTION_BYTES );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Bytes used for content type detection: {} and as UTF-8: {}",
                              firstBytes,
                              new String( firstBytes,
                                          StandardCharsets.UTF_8 ) );
            }

            inputStream.reset();
        }
        catch ( TikaException | IOException e )
        {
            throw new PreIngestException( "Could not read from stream associated with '"
                                          + uri
                                          + "':",
                                          e );
        }

        return Pair.of( detectedMediaType, firstBytes );
    }

    /**
     * Examines the first few bytes of an XML source for its detailed disposition.
     * @param firstBytes the first few bytes
     * @param uri the URI to help with logging
     * @return the detailed disposition
     */

    private static DataDisposition getDispositionOfXmlSubtype( byte[] firstBytes, URI uri )
    {
        DataDisposition innerDisposition = DataDisposition.UNKNOWN;

        Charset xmlCharset = DataSource.getXmlCharsetFromBom( firstBytes );
        String start = new String( firstBytes, xmlCharset );

        if ( start.contains( "fews/PI" ) && start.contains( "<TimeSeries" ) )
        {
            innerDisposition = DataDisposition.XML_PI_TIMESERIES;
        }
        else
        {
            LOGGER.warn( "Found XML document but it did not appear to be a FEWS PI TimeSeries: '{}'",
                         uri );
        }

        return innerDisposition;
    }

    /**
     * Examines the first few bytes of a JSON source for its detailed disposition.
     * @param firstBytes the first few bytes
     * @param uri the URI to help with logging
     * @return the detailed disposition
     */

    private static DataDisposition getDispositionOfJsonSubtype( byte[] firstBytes, URI uri )
    {
        DataDisposition innerDisposition = DataDisposition.UNKNOWN;

        // This could be either WRDS json or USGS-style json.
        Charset jsonCharset = DataSource.getJsonCharset( firstBytes );

        if ( jsonCharset == null )
        {
            throw new PreIngestException( "Unable to detect charset for JSON document starting with bytes "
                                          + Arrays.toString( firstBytes ) );
        }

        String start = new String( firstBytes, jsonCharset );

        if ( start.contains( "org.cuahsi.waterml" ) )
        {
            innerDisposition = DataDisposition.JSON_WATERML;
        }
        // There is a WRDS json format for thresholds (it's not timeseries).
        else if ( !start.contains( "\"threshold" ) )
        {
            if ( start.contains( "wrds" ) && start.contains( "nwm" ) )
            {
                innerDisposition = DataDisposition.JSON_WRDS_NWM;
            }
            else if ( start.contains( "\"header\":" ) )
            {
                innerDisposition = DataDisposition.JSON_WRDS_AHPS;
            }
        }
        else
        {
            LOGGER.warn( "Found JSON document but it did not appear to be one WRES could parse: '{}'",
                         uri );
        }

        return innerDisposition;
    }

    /**
     * Examines the first few bytes of a CSV source for its detailed disposition.
     * @param firstBytes the first few bytes
     * @param uri the URI to help with logging
     * @return the detailed disposition
     */

    private static DataDisposition getDispositionOfCsvSubtype( byte[] firstBytes, URI uri )
    {
        DataDisposition innerDisposition = DataDisposition.UNKNOWN;

        // Our CSV reader used default charset as of 5.10, changing it to
        // UTF-8 as of this commit, see CSVDataProvider class.
        String start = new String( firstBytes, StandardCharsets.UTF_8 );
        String[] wresCsvRequiredHeaders = { "value_date", "variable_name",
                "location", "measurement_unit",
                "value" };
        boolean missingHeaders = false;

        for ( String requiredHeader : wresCsvRequiredHeaders )
        {
            if ( !start.contains( requiredHeader ) )
            {
                LOGGER.warn( "Found CSV document but it did not contain required column '{}': '{}'",
                             requiredHeader,
                             uri );
                missingHeaders = true;
            }
        }

        if ( !missingHeaders )
        {
            innerDisposition = DataDisposition.CSV_WRES;
        }

        return innerDisposition;
    }

    /**
     * Examines the first few bytes of a source identified as plain text for its detailed disposition.
     * @param firstBytes the first few bytes
     * @param uri the URI to help with logging
     * @return the detailed disposition
     */

    private static DataDisposition getDispositionOfTextTypeAndPlainSubtype( byte[] firstBytes, URI uri )
    {
        DataDisposition innerDisposition = DataDisposition.UNKNOWN;

        if ( DataSource.detectDatacardFromTextPlain( firstBytes, uri ) )
        {
            innerDisposition = DataDisposition.DATACARD;
        }
        // Sometimes a tar file is returned as text/plain from tika
        else if ( DataSource.detectTarFileFromTextPlain( firstBytes, uri ) )
        {
            LOGGER.warn( "Using a potentially corrupt tarball: '{}'", uri );
            innerDisposition = DataDisposition.TARBALL;
        }
        // Sometimes a CSV file is returned as text/plain from tika
        else if ( DataSource.detectCsvFromTextPlain( firstBytes, uri ) )
        {
            innerDisposition = DataDisposition.CSV_WRES;
        }
        else
        {
            LOGGER.warn( "Found a text/plain document, but it did not appear to be NWS datacard: '{}'",
                         uri );
        }

        return innerDisposition;
    }

    /**
     * Examines the first few bytes of a source identified as Fast Infoset for its detailed disposition.
     * @param firstBytes the first few bytes
     * @param uri the URI to help with logging
     * @return the detailed disposition
     */

    private static DataDisposition getDispositionOfFastInfosetSubtype( byte[] firstBytes, URI uri )
    {
        DataDisposition innerDisposition = DataDisposition.UNKNOWN;

        // It is nominally Fast Infoset encoded XML, but is it Published Interface XML?
        if ( DataSource.isFastInfosetPixml( firstBytes, uri ) )
        {
            innerDisposition = DataDisposition.XML_FI_TIMESERIES;
        }
        else
        {
            LOGGER.warn( "Found an application/fastinfosetxml document, but it did not appear to be PI-XML: '{}'",
                         uri );
        }

        return innerDisposition;
    }

    /**
     * Get the charset for a JSON document.
     * <a href="https://datatracker.ietf.org/doc/html/rfc4627#section-3">https://datatracker.ietf.org/doc/html/rfc4627#section-3</a>
     *            00 00 00 xx  UTF-32BE
     *            00 xx 00 xx  UTF-16BE
     *            xx 00 00 00  UTF-32LE
     *            xx 00 xx 00  UTF-16LE
     *            xx xx xx xx  UTF-8
     * @param bytes The leading bytes, at least 4 of them.
     * @return The detected charset, null if not detected or not supported.
     * @throws IllegalArgumentException when bytes length is under 4.
     */

    private static Charset getJsonCharset( byte[] bytes )
    {
        Objects.requireNonNull( bytes );

        if ( bytes.length < 4 )
        {
            throw new IllegalArgumentException( "Four or more bytes must be given. Got "
                                                + bytes.length );
        }

        if ( bytes[0] != 0 )
        {
            if ( bytes[1] != 0 && bytes[2] != 0 && bytes[3] != 0 )
            {
                return StandardCharsets.UTF_8;
            }
            else if ( bytes[1] == 0 && bytes[2] != 0 && bytes[3] == 0 )
            {
                return StandardCharsets.UTF_16LE;
            }
            else if ( bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 0 )
            {
                return Charset.forName( "UTF-32LE" );
            }
        }
        else
        {
            if ( bytes[1] != 0 && bytes[2] == 0 && bytes[3] != 0 )
            {
                return StandardCharsets.UTF_16BE;
            }
            else if ( bytes[1] == 0 && bytes[2] == 0 && bytes[3] != 0 )
            {
                return Charset.forName( "UTF-32BE" );
            }
        }

        return null;
    }


    /**
     * Get the UTF charset from the byte order mark (first two bytes). Here
     * assume that no byte order mark means UTF-8.
     * <a href="https://www.w3.org/TR/xml/#charencoding">https://www.w3.org/TR/xml/#charencoding</a>
     * <a href="https://en.wikipedia.org/wiki/Byte_order_mark#UTF-16">https://en.wikipedia.org/wiki/Byte_order_mark#UTF-16</a>
     * @param bytes The leading bytes, at least 2 of them.
     * @return The detected charset, null if not detected or not supported.
     * @throws IllegalArgumentException when bytes length is under 2.
     */

    private static Charset getXmlCharsetFromBom( byte[] bytes )
    {
        Objects.requireNonNull( bytes );

        if ( bytes.length < 2 )
        {
            throw new IllegalArgumentException( "Two or more bytes must be given. Got "
                                                + bytes.length );
        }

        if ( Byte.toUnsignedInt( bytes[0] ) == 0xFE
             && Byte.toUnsignedInt( bytes[1] ) == 0xFF )
        {
            return StandardCharsets.UTF_16BE;
        }
        else if ( Byte.toUnsignedInt( bytes[0] ) == 0xFF
                  && Byte.toUnsignedInt( bytes[1] ) == 0xFE )
        {
            return StandardCharsets.UTF_16LE;
        }

        return StandardCharsets.UTF_8;
    }

    /**
     * Determines whether application/fastinfoset formatted data is in Published Interface XML format.
     * @param firstBytes the first bytes to use for content type detection
     * @param uri the URI
     * @return true if the data is PI-XML, false otherwise
     */

    private static boolean isFastInfosetPixml( byte[] firstBytes, URI uri )
    {
        try ( InputStream stream = new ByteArrayInputStream( firstBytes ) )
        {
            XMLStreamReader reader = new StAXDocumentParser( stream );

            // Look for an expected tag
            while ( reader.hasNext() )
            {
                reader.nextTag();

                if ( reader.getLocalName().equalsIgnoreCase( "TimeSeries" ) )
                {
                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "While inspecting {} closely, discovered that it was Fast Infoset encoded "
                                      + "PI-XML.",
                                      uri );
                    }

                    return true;
                }
            }
        }
        catch ( IOException | XMLStreamException e )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                String message = "Failed to detect Fast Infoset encoded PI-XML in " + uri;
                LOGGER.debug( message, e );
            }
        }

        return false;
    }

    /**
     * Since tika sometimes reports tarballs as text/plain, do some more work
     * to detect.
     * @param firstBytes The bytes to use for detection
     * @param uri The URI to use for logging
     * @return true when a tarball, false otherwise
     */

    private static boolean detectTarFileFromTextPlain( byte[] firstBytes,
                                                       URI uri )
    {
        if ( firstBytes.length <= 512 )
        {
            LOGGER.debug( "Length was {}, tar would be at least 512 bytes.",
                          firstBytes.length );
            return false;
        }

        int countFor = 0;
        int countAgainst = 0;

        LOGGER.debug( "There are more than 512 bytes, maybe tar. {}",
                      uri );

        byte firstByte = firstBytes[0];
        // Tar header should have a filename with printable chars.
        if ( firstByte >= 32 && firstByte < 127 )
        {
            countFor++;
            LOGGER.debug( "First byte {} is printable.", firstBytes[0] );
        }
        else
        {
            countAgainst++;
            LOGGER.debug( "First byte {} is NOT printable.",
                          Character.getNumericValue( firstBytes[0] ) );
        }

        byte[] byte124to135 = Arrays.copyOfRange( firstBytes, 124, 135 );

        // These bytes can be padded with 0x0 or space or be ascii numerals:
        Set<Integer> validBytesIn124to135 = Set.of( 0,
                                                    32,
                                                    48,
                                                    49,
                                                    50,
                                                    51,
                                                    52,
                                                    53,
                                                    54,
                                                    55,
                                                    56,
                                                    57 );
        Set<Byte> invalidBytesFoundIn124to135 = new HashSet<>();
        for ( byte b : byte124to135 )
        {
            int intValue = Byte.toUnsignedInt( b );

            if ( !validBytesIn124to135.contains( intValue ) )
            {
                invalidBytesFoundIn124to135.add( b );
            }
        }

        if ( invalidBytesFoundIn124to135.isEmpty() )
        {
            countFor++;
            LOGGER.debug( "There were no invalid bytes in range 124 to 135" );
        }
        else
        {
            countAgainst++;
            LOGGER.debug( "INVALID bytes in range 124 to 135 found: {}",
                          invalidBytesFoundIn124to135 );
        }

        int byte135 = Byte.toUnsignedInt( firstBytes[135] );

        if ( byte135 == 0 || byte135 == 32 )
        {
            countFor++;
            LOGGER.debug( "Byte 135 was a valid tar value: 0 or space: {}.",
                          byte135 );
        }
        else
        {
            countAgainst++;
            LOGGER.debug( "Byte 135 was NOT a 0 or space: {}", byte135 );
        }

        int byte156 = Byte.toUnsignedInt( firstBytes[156] );
        Set<Integer> validBytesAt156 = Set.of( 0,
                                               1,
                                               2,
                                               48,
                                               49,
                                               50,
                                               51,
                                               52,
                                               53,
                                               54,
                                               55,
                                               103,
                                               12,
                                               65,
                                               66,
                                               67,
                                               68,
                                               69,
                                               70,
                                               71,
                                               72,
                                               73,
                                               74,
                                               75,
                                               76,
                                               77,
                                               78,
                                               79,
                                               80,
                                               81,
                                               82,
                                               83,
                                               84,
                                               85,
                                               86,
                                               87,
                                               88,
                                               89,
                                               90 );

        if ( validBytesAt156.contains( byte156 ) )
        {
            countFor++;
            LOGGER.debug( "Byte 156 was valid tar value: {}", byte156 );
        }
        else
        {
            countAgainst++;
            LOGGER.debug( "Byte 156 was NOT valid tar value: {}", byte156 );
        }

        return countAgainst == 0 && countFor > 0;
    }

    /**
     * Attempts to identify a plain text byte array as {@link DataDisposition#CSV_WRES}.
     * @param firstBytes the bytes to detect
     * @param uri the uri
     * @return whether the format is {@link DataDisposition#CSV_WRES}
     */

    private static boolean detectCsvFromTextPlain( byte[] firstBytes, URI uri )
    {
        String start = new String( firstBytes, StandardCharsets.UTF_8 );
        String[] wresCsvRequiredHeaders = { "value_date", "variable_name",
                "location", "measurement_unit",
                "value" };

        for ( String requiredHeader : wresCsvRequiredHeaders )
        {
            if ( !start.contains( requiredHeader ) )
            {
                LOGGER.debug( "Found a plain text file {} without an expected WRES CSV header field {}, so assuming "
                              + "this is not WRES CSV or is malformed.",
                              uri,
                              requiredHeader );

                return false;
            }
        }

        return true;
    }

    /**
     * Attempts to identify a plain text byte array as {@link DataDisposition#DATACARD}.
     * @param firstBytes the bytes to detect
     * @param uri the uri
     * @return whether the format is {@link DataDisposition#DATACARD}
     */

    private static boolean detectDatacardFromTextPlain( byte[] firstBytes, URI uri )
    {
        // Datacard is in ascii, so treat it as ascii for further checks.
        // Also, a tar header (falsely detected as text/plain) is in ascii.
        String start = new String( firstBytes, StandardCharsets.US_ASCII );

        // Obtain the first two lines that do not begin with a comment ($) character. There must be two of them.
        // Furthermore, the second line must contain the elements described here:
        // as documented here: https://www.weather.gov/media/owp/oh/hrl/docs/72datacard.pdf
        // This should be a good enough hook for content type detection

        // Split by newline characters. Ambiguous whether \r\n is allowed or only \n, so allow both
        String[] lines = start.split( "\\r?\\n|\\r" );

        // Filter any lines that begin with a comment character, $
        List<String> filtered = Arrays.stream( lines )
                                      .filter( next -> !next.startsWith( "$" ) )
                                      .toList();

        if ( filtered.size() < 2 )
        {
            LOGGER.debug( "While checking {} for a Datacard string, discovered fewer than two non-comment lines, so "
                          + "this is not Datacard. A comment line begins with $.",
                          uri );

            return false;
        }

        String lineTwo = filtered.get( 1 );

        // Strip any trailing whitespace
        lineTwo = lineTwo.stripTrailing();

        // Must be between 25 and 32 characters because the format element sits in this char range and is the last part 
        int charCount = lineTwo.length();

        if ( charCount < 25 || charCount > 32 )
        {
            LOGGER.debug( WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                          + "contained a string of unexpected length for Datacard. The expected length is [25,32] and "
                          + "the actual length was {}. Thus, the source was not identified as Datacard. A comment line "
                          + "begins with $.",
                          uri,
                          charCount );

            return false;
        }

        // Check the month/year components at the expected character positions, trimmed for whitespace
        String first = lineTwo.substring( 0, 2 )
                              .trim();
        String second = lineTwo.substring( 4, 8 )
                               .trim();
        String third = lineTwo.substring( 9, 11 )
                              .trim();
        String fourth = lineTwo.substring( 14, 18 )
                               .trim();

        String message = WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                         + "contained a string with unexpected contents. Expected a two-digit month, followed by a "
                         + "four-digit year, followed by a two-digit month and a final four-digit year. Thus, the "
                         + "source was not identified as Datacard. A comment line begins with $.";

        try
        {
            int firstMonth = Integer.parseInt( first );

            if ( firstMonth < 1 || firstMonth > 12 )
            {
                LOGGER.debug( WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                              + "contained a string with unexpected contents. Expected a two-digit month in positions "
                              + "1-2. Thus, the source was not identified as Datacard. A comment line begins with $.",
                              uri );

                return false;
            }

            int firstYear = Integer.parseInt( second );

            if ( Integer.toString( firstYear )
                        .length() != 4 )
            {
                LOGGER.debug( WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                              + "contained a string with unexpected contents. Expected a four-digit year in positions "
                              + "5-8. Thus, the source was not identified as Datacard. A comment line begins with $.",
                              uri );

                return false;
            }

            int secondMonth = Integer.parseInt( third );

            if ( secondMonth < 1 || secondMonth > 12 )
            {
                LOGGER.debug( WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                              + "contained a string with unexpected contents. Expected a two-digit month in positions "
                              + "10-11. Thus, the source was not identified as Datacard. A comment line begins with $.",
                              uri );

                return false;
            }

            int secondYear = Integer.parseInt( fourth );

            if ( Integer.toString( secondYear )
                        .length() != 4 )
            {
                LOGGER.debug( WHILE_CHECKING_FOR_A_DATACARD_STRING_DISCOVERED_THAT_THE_SECOND_NON_COMMENT_LINE
                              + "contained a string with unexpected contents. Expected a four-digit year in positions "
                              + "15-18. Thus, the source was not identified as Datacard. A comment line begins with $.",
                              uri );

                return false;
            }
        }
        catch ( NumberFormatException e )
        {
            LOGGER.debug( message, uri );

            return false;
        }

        LOGGER.debug( "Data source {} was identified as Datacard.", uri );

        return true;
    }

}
