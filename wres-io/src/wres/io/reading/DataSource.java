package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.time.TimeSeries;

public class DataSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DataSource.class );

    /**
     * The disposition of the original or generated-from-original source
     * declaration. This is for software to determine, not the user. The COMPLEX
     * disposition is for readers that read timeseries from across multiple
     * streams or multiply one declaration into many streams, e.g. NWM or NWIS.
     * The FILE_OR_DIRECTORY means the URI protocol made it look like a file or
     * directory but it has yet to be examined. The bulk of remaining
     * dispositions can be auto-detected and drive which source or reader class
     * will handle the data. UNKNOWN indicates the data could not be detected
     * and should be ignored by WRES.
     */

    public enum DataDisposition
    {
        /** The data source involves multiple streams and a complex reader. */
        COMPLEX,
        /** The data source is a directory to walk or a file to check. */
        FILE_OR_DIRECTORY,
        /** The data has been detected as a gzip stream. */
        GZIP,
        /** The data has been detected as a tarball. */
        TARBALL,
        /** The data has been detected as a pi-xml timeseries stream. */
        XML_PI_TIMESERIES,
        /** The data has been detected as a datacard stream. */
        DATACARD,
        /** The data has been detected as a netCDF blob. */
        NETCDF_GRIDDED,
        /** The data has been detected as a json, wrds/nwm stream. */
        JSON_WRDS_NWM,
        /** The data has been detected as a json, wrds/ahps stream. */
        JSON_WRDS_AHPS,
        /** The data has been detected as a json, waterml stream. */
        JSON_WATERML,
        /** The data has been detected as a csv, wres stream. */
        CSV_WRES,
        /** The data stream did not fit into any of the other categories. */
        UNKNOWN
    }

    /**
     * The count of bytes to use for detecting WRES-compatible type.
     */

    static final int DETECTION_BYTES = 1024;

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

    private final Set<LeftOrRightOrBaseline> links;

    /**
     * URI of the source. Required when ingesting, but null when this object is
     * used in {@link IngestResult} to report back that ingest is complete, for
     * heap savings.
     */

    private final URI uri;

    /**
     * A raw TimeSeries when the source has already been read, null otherwise.
     */

    private final TimeSeries<?> timeSeries;


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
     * @throws NullPointerException if any input is null
     * @return The newly created DataSource.
     */

    public static DataSource of( DataDisposition disposition,
                                 DataSourceConfig.Source source,
                                 DataSourceConfig context,
                                 Set<LeftOrRightOrBaseline> links,
                                 URI uri )
    {
        Objects.requireNonNull( disposition );
        Objects.requireNonNull( uri );
        return new DataSource( disposition,
                               source,
                               context,
                               links,
                               uri,
                               null );
    }


    /**
     * Create a data source to load into <code>wres.Source</code> with an
     * already-read {@link TimeSeries}, with optional links to create in
     * <code>wres.ProjectSource</code>. If the source is used only once in the
     * declaration, there will be no additional links and the set of links
     * should be empty. The evaluated path to the source may not match the URI
     * within the source, because the path has been evaluated. For example,
     * evaluation means to decompose a source directory into separate paths to
     * each file that must be loaded. Each file has a separate
     * {@link DataSource}. For each of those decomposed paths, there is only one
     * {@link DataSourceConfig.Source}.
     *
     * @param disposition the disposition of the data source or data inside
     * @param source the source to load
     * @param context the context in which the source appears
     * @param links the optional links to create
     * @param uri the uri for the source
     * @param timeSeries The {@link TimeSeries} already-read from the source.
     * @throws NullPointerException When source, context, links, or uri are null
     * @return The newly created DataSource.
     */

    public static DataSource of( DataDisposition disposition,
                                 DataSourceConfig.Source source,
                                 DataSourceConfig context,
                                 Set<LeftOrRightOrBaseline> links,
                                 URI uri,
                                 TimeSeries<?> timeSeries )
    {
        Objects.requireNonNull( disposition );
        Objects.requireNonNull( uri );
        Objects.requireNonNull( source );
        return new DataSource( disposition,
                               source,
                               context,
                               links,
                               uri,
                               timeSeries );
    }

    /**
     * Create a source.
     * @param disposition the disposition of the data source or data inside
     * @param source the source
     * @param context the context in which the source appears
     * @param links the links
     * @param uri the uri
     */

    private DataSource( DataDisposition disposition,
                        DataSourceConfig.Source source,
                        DataSourceConfig context,
                        Set<LeftOrRightOrBaseline> links,
                        URI uri,
                        TimeSeries<?> timeSeries )
    {
        Objects.requireNonNull( disposition );
        Objects.requireNonNull( context );
        Objects.requireNonNull( links );

        this.disposition = disposition;
        this.source = source;
        this.context = context;

        if ( links.equals( Collections.emptySet() ) )
        {
            this.links = links;
        }
        else
        {
            this.links = Collections.unmodifiableSet( links );
        }

        this.uri = uri;
        this.timeSeries = timeSeries;
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

    public Set<LeftOrRightOrBaseline> getLinks()
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

    boolean hasSourcePath()
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
     * Returns the {@link TimeSeries} that was already read from the source.
     * @return The timeseries or null if none was provided on construction.
     */
    public TimeSeries<?> getTimeSeries()
    {
        return this.timeSeries;
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
               links.equals( that.links ) &&
               Objects.equals( context, that.context ) &&
               Objects.equals( uri, that.uri ) &&
               Objects.equals( timeSeries, that.timeSeries );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( context, source, links, uri, timeSeries );
    }

    @Override
    public String toString()
    {
        // Improved for #63493
        
        StringJoiner joiner = new StringJoiner( ";", "(", ")" );

        joiner.add( "Disposition: " + this.getDisposition() );
        joiner.add( " URI: " + this.getUri() );
        joiner.add( " Type: " + this.getContext().getType() );

        if ( !this.getLinks().isEmpty() )
        {
            joiner.add( " Links to other contexts: " + this.getLinks() );
        }

        if ( Objects.nonNull( this.getTimeSeries() ) )
        {
            String timeseries =  " TimeSeries with ";

            if ( Objects.nonNull( this.getTimeSeries().getEvents() ) )
            {
                timeseries += this.getTimeSeries()
                                  .getEvents()
                                  .size();
            }
            else
            {
                timeseries += " no ";
            }

            timeseries += " events: ";
            timeseries +=  this.getTimeSeries()
                               .getMetadata()
                               .toString();

            joiner.add( timeseries );
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
     * Only returns values for simple types, will not return COMPLEX, will not
     * return FILE_OR_DIRECTORY.
     * @param inputStream The stream to examine, at position 0, will be kept
     *                    there, must support mark/reset.
     * @param uri A real or fake URI (basically a name) to log with messages.
     * @return The WRES guess, or UNKNOWN if unknown.
     * @throws PreIngestException When format detection encounters an exception.
     * @throws UnsupportedOperationException When !inputStream.markSupported()
     */

    public static DataDisposition detectFormat( InputStream inputStream,
                                                URI uri )
    {
        LOGGER.debug( "detectFormat called on input stream for {}", uri );
        MediaType detectedMediaType;
        Metadata metadata = new Metadata();
        metadata.set( Metadata.RESOURCE_NAME_KEY,
                      uri.toString() );
        byte[] firstBytes;
        DataDisposition disposition = DataDisposition.UNKNOWN;

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
                                                         + "BufferedInputStream"
                                                         + "or TikaInputStream "
                                                         + "instead." );
            }

            firstBytes = inputStream.readNBytes( DETECTION_BYTES );

            inputStream.reset();
        }
        catch ( TikaException | IOException e )
        {
            throw new PreIngestException( "Could not read from stream associated with '"
                                          + uri + "':", e );
        }

        String mediaType = detectedMediaType.getType()
                                            .toLowerCase();
        String subtype = detectedMediaType.getSubtype()
                                          .toLowerCase();
        LOGGER.debug( "For data labeled {}, mediaType={}, subtype={}",
                      uri, mediaType, subtype );

        if ( firstBytes.length < 4 )
        {
            LOGGER.warn( "Found document with only {} bytes: '{}'",
                         firstBytes.length, uri );
        }
        else if ( subtype.equals( "xml" ) )
        {
            Charset xmlCharset = DataSource.getXmlCharsetFromBom( firstBytes );
            String start = new String( firstBytes, xmlCharset );

            if ( start.contains( "fews/PI" ) && start.contains( "<TimeSeries" ) )
            {
                disposition = DataDisposition.XML_PI_TIMESERIES;
            }
            else
            {
                LOGGER.warn( "Found XML document but it did not appear to be a FEWS PI TimeSeries: '{}'",
                             uri );
            }
        }
        else if ( subtype.equals( "json" ) && firstBytes.length > 4 )
        {
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
                disposition = DataDisposition.JSON_WATERML;
            }
            // There is a WRDS json format for thresholds (it's not timeseries).
            else if ( !start.contains( "\"threshold" ) )
            {
                if ( start.contains( "wrds" ) && start.contains( "nwm" ) )
                {
                    disposition = DataDisposition.JSON_WRDS_NWM;
                }
                else if ( start.contains( "\"header\":" ) )
                {
                    disposition = DataDisposition.JSON_WRDS_AHPS;
                }
            }
            else
            {
                LOGGER.warn( "Found JSON document but it did not appear to be one WRES could parse: '{}'",
                             uri );
            }
        }
        else if ( subtype.equals( "csv" ) )
        {
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
                                 requiredHeader, uri );
                    missingHeaders = true;
                }
            }

            if ( !missingHeaders )
            {
                disposition = DataDisposition.CSV_WRES;
            }
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
            // Datacard is in ascii, so treat it as ascii for further checks.
            // Also, a tar header (falsely detected as text/plain) is in ascii.
            String start = new String( firstBytes, StandardCharsets.US_ASCII );

            // Datacard always starts with $ and has max 80 chars in a line.
            // Ambiguous whether \r\n is allowed or only \n.
            int indexOfFirstNewLine = start.indexOf( '\n' );
            int indexOfSecondNewLine = start.indexOf( '\n',
                                                      indexOfFirstNewLine + 1 );
            if ( start.startsWith( "$" )
                 && indexOfFirstNewLine > 0
                 && indexOfFirstNewLine <= 80
                 // We assume there are always two newlines here.
                 && indexOfSecondNewLine > indexOfFirstNewLine + 1
                 && indexOfSecondNewLine <= indexOfFirstNewLine + 81 )
            {
                disposition = DataDisposition.DATACARD;
            }
            // Sometimes a tar file is returned as text/plain from tika
            else if ( DataSource.detectTarFileFromTextPlain( firstBytes, uri ) )
            {
                LOGGER.warn( "Using a potentially corrupt tarball: '{}'", uri );
                disposition = DataDisposition.TARBALL;
            }
            else
            {
                LOGGER.warn( "Found text/plain document but it did not appear to be NWS datacard: '{}'",
                             uri );
            }
        }

        LOGGER.debug( "Detected {} for {}", disposition, uri );
        return disposition;
    }


    /**
     * Get the charset for a JSON document.
     * https://datatracker.ietf.org/doc/html/rfc4627#section-3
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
     * https://www.w3.org/TR/xml/#charencoding
     * https://en.wikipedia.org/wiki/Byte_order_mark#UTF-16
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
            return StandardCharsets.UTF_16BE;
        }

        return StandardCharsets.UTF_8;
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

        byte[] byte124to135 = Arrays.copyOfRange( firstBytes, 124, 135  );

        // These bytes can be padded with 0x0 or space or be ascii numerals:
        Set<Integer> validBytesIn124to135 = Set.of( 0, 32, 48, 49, 50, 51, 52,
                                                    53, 54, 55, 56, 57 );
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
        Set<Integer> validBytesAt156 = Set.of( 0, 1, 2, 48, 49, 50, 51, 52, 53,
                                               54, 55, 103, 12, 65, 66, 67, 68,
                                               69,70, 71, 72, 73, 74, 75, 76,
                                               77, 78, 79, 80, 81, 82, 83, 84,
                                               85, 86, 87, 88, 89, 90 );

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
}
