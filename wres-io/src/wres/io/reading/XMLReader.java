package wres.io.reading;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Strings;

/**
 * @author Tubbs
 *
 */
public abstract class XMLReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( XMLReader.class );

    private final String filename;
    private final InputStream inputStream;
    private final XMLInputFactory factory;

    /**
     * Convenience constructor - finds in either the classpath or filesystem.
     * @param filename the file name to look for on the classpath or filesystem.
     * @throws IOException when the file cannot be found.
     */
    protected XMLReader( String filename )
            throws IOException
    {
        this( filename, null );
    }

    /**
     * Create an XMLReader.
     * <br>
     * By the time this constructor is finished, we have an open InputStream
     * for the file name specified.
     * @param fileName the name of the file to read, non-null
     * @param inputStream null or optionat inputstream to read from
     * @throws IOException when anything goes wrong
     */
    protected XMLReader( String fileName, InputStream inputStream )
            throws IOException
    {
        Objects.requireNonNull( fileName );

        this.filename = fileName;
        this.factory = XMLInputFactory.newFactory();

        InputStream possibleInputStream = inputStream;

        if ( possibleInputStream != null )
        {
            // Caller set up the input stream in advance, use it.
            this.inputStream = inputStream;
            LOGGER.debug( "Caller set an InputStream" );
        }
        else
        {
            // No input stream was passed in, attempt to get from the classpath.
            possibleInputStream = XMLReader.getFile( this.filename );

            // possibleInputStream can still be null at this point, meaning that
            // the resource could not be found on the classpath.
            if ( possibleInputStream != null )
            {
                // Successfully found on classpath.
                this.inputStream = possibleInputStream;
                LOGGER.debug( "Found {} on classpath.", this.filename );
            }
            else
            {
                // Not found on classpath.
                // Therefore, attempt to create one from the filesystem now.
                possibleInputStream = new FileInputStream( this.filename );

                if ( this.filename.endsWith( ".gz" ) )
                {
                    this.inputStream =
                            new GZIPInputStream( possibleInputStream );
                    LOGGER.debug( "Found gzip file {}.", this.filename );
                }
                else
                {
                    this.inputStream = possibleInputStream;
                    LOGGER.debug( "Found file {}.", this.filename );
                }
            }
        }
    }


    /**
     * A no-op constructor so that SystemSettings can successfully inherit
     * from this class but not use its functionality (due to failed reading).
     */
    protected XMLReader()
    {
        this.filename = null;
        this.inputStream = null;
        this.factory = null;
    }

    protected String getFilename()
    {
        return filename;
    }

    /**
     * Attempt to find a resource on the classpath. If not found, return null.
     * @param resourceName the resource name
     * @return InputStream on success, null on failure to find.
     * @throws IOException when getting a gzipped resource fails
     */
    private static InputStream getFile( String resourceName ) throws IOException
    {
        InputStream stream = XMLReader.class.getClassLoader()
                                            .getResourceAsStream( resourceName );

        if ( stream != null && resourceName.endsWith( ".gz" ) )
        {
            stream = new GZIPInputStream( stream );
        }
        return stream;
    }

    public void parse() throws IOException
    {
        XMLStreamReader reader = null;
        try
        {
            reader = createReader();

            if ( reader == null )
            {
                throw new XMLStreamException( "A reader for the XML named '" +
                                              this.getFilename()
                                              +
                                              "' could not be created." );
            }

            while ( reader.hasNext() )
            {
                parseElement( reader );
                if ( reader.hasNext() )
                {
                    reader.next();
                }
            }

            reader.close();
            completeParsing();
        }
        catch ( XMLStreamException error )
        {
            throw new IOException( "Could not parse file " + this.filename, error );
        }
        finally
        {
            if ( reader != null )
            {
                try
                {
                    reader.close();
                }
                catch ( XMLStreamException xse )
                {
                    // not much we can do at this point
                    this.getLogger().warn( "Exception while closing file {}.",
                                           this.filename,
                                           xse );
                }
            }
        }
    }

    protected void completeParsing()
            throws IOException
    {
        // Optional for subclasses to implement
    }

    private XMLStreamReader createReader() throws IOException, XMLStreamException
    {
        return factory.createXMLStreamReader( inputStream );
    }

    public String getRawXML() throws IOException, XMLStreamException, TransformerException
    {
        XMLStreamReader reader = createReader();
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        StringWriter stringWriter = new StringWriter();
        transformer.transform( new StAXSource( reader ), new StreamResult( stringWriter ) );
        return stringWriter.toString();
    }

    protected void parseElement( XMLStreamReader reader ) throws IOException
    {
        switch ( reader.getEventType() )
        {
            case XMLStreamConstants.START_DOCUMENT:
                this.getLogger().trace( "Start of the document" );
                break;
            case XMLStreamConstants.START_ELEMENT:
                this.getLogger().trace( "Start element = '" + reader.getLocalName() + "'" );
                break;
            case XMLStreamConstants.CHARACTERS:
                int beginIndex = reader.getTextStart();
                int endIndex = reader.getTextLength();
                String value = new String( reader.getTextCharacters(), beginIndex, endIndex ).trim();

                if ( Strings.hasValue( value ) )
                {
                    this.getLogger().trace( "Value = '{}'", value );
                }

                break;
            case XMLStreamConstants.END_ELEMENT:
                this.getLogger().trace( "End element = '" + reader.getLocalName() + "'" );
                break;
            case XMLStreamConstants.COMMENT:
                if ( reader.hasText() )
                {
                    this.getLogger().trace( reader.getText() );
                }
                break;
            default:
                throw new IOException( "The event: '" +
                                       reader.getEventType()
                                       +
                                       "' is not parsed by default." );
        }
    }

    protected abstract Logger getLogger();
}
