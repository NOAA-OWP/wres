package wres.system.xml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import com.sun.xml.fastinfoset.stax.StAXDocumentParser; //NOSONAR

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Christopher Tubbs
 */
public abstract class XMLReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( XMLReader.class );
    private static final XMLInputFactory DEFAULT_FACTORY = XMLInputFactory.newFactory();

    private final URI filename;
    private final InputStream inputStream;
    private final XMLInputFactory factory;
    private final boolean fastInfoset;

    /**
     * Create an XMLReader.
     * <br>
     * By the time this constructor is finished, we have an open InputStream
     * for the file name specified.
     * @param fileName the name of the file to read, non-null
     * @param inputStream null or optionat inputstream to read from
     * @throws IOException when anything goes wrong
     */
    protected XMLReader( URI fileName, InputStream inputStream )
            throws IOException
    {
        this( fileName, inputStream, false );
    }

    /**
     * Create an XMLReader.
     * <br>
     * By the time this constructor is finished, we have an open InputStream
     * for the file name specified.
     * @param fileName the name of the file to read, non-null
     * @param inputStream null or optionat inputstream to read from
     * @param fastInfoset is true if the file is fast-infoset encoded
     * @throws IOException when anything goes wrong
     */
    protected XMLReader( URI fileName, InputStream inputStream, boolean fastInfoset )
            throws IOException
    {
        Objects.requireNonNull( fileName );

        this.filename = fileName;
        this.factory = DEFAULT_FACTORY;
        this.fastInfoset = fastInfoset;

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
                this.inputStream = new FileInputStream( this.filename.getPath() );
                LOGGER.debug( "Found file {}.", this.filename );
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
        this.fastInfoset = false;
    }

    /**
     * @return the file name
     */
    protected URI getFilename()
    {
        return filename;
    }

    /**
     * Attempt to find a resource on the classpath. If not found, return null.
     * @param resourceName the resource name
     * @return InputStream on success, null on failure to find.
     */
    private static InputStream getFile( URI resourceName )
    {
        return XMLReader.class.getClassLoader()
                              .getResourceAsStream( resourceName.getPath() );
    }

    /**
     * @return true if the file is fast-infoset encoded, false otherwise
     */
    private boolean isFastInfoset()
    {
        return this.fastInfoset;
    }

    /**
     * Parse the XML.
     * @throws IOException if the XML could not be parsed
     */
    public void parse() throws IOException
    {
        XMLStreamReader reader = null;
        try
        {
            reader = this.createReader();

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
                    LOGGER.warn( "Exception while closing file {}.",
                                 this.filename,
                                 xse );
                }
            }
        }
    }

    /**
     * @throws IOException if the parsing failed
     */
    protected void completeParsing()
            throws IOException
    {
        // Optional for subclasses to implement
    }

    private XMLStreamReader createReader() throws XMLStreamException
    {
        if ( this.isFastInfoset() )
        {
            return new StAXDocumentParser( this.inputStream );
        }
        else
        {
            return this.factory.createXMLStreamReader( this.inputStream );
        }
    }

    /**
     * @param reader the reader
     * @throws IOException if the element could not be parsed
     */
    protected void parseElement( XMLStreamReader reader ) throws IOException
    {
        switch ( reader.getEventType() )
        {
            case XMLStreamConstants.START_DOCUMENT -> LOGGER.trace( "Start of the document" );
            case XMLStreamConstants.START_ELEMENT -> LOGGER.trace( "Start element = '{}'", reader.getLocalName() );
            case XMLStreamConstants.CHARACTERS ->
            {
                int beginIndex = reader.getTextStart();
                int endIndex = reader.getTextLength();
                String value = new String( reader.getTextCharacters(), beginIndex, endIndex ).trim();
                if ( LOGGER.isTraceEnabled() && !value.isBlank() )
                {
                    LOGGER.trace( "Value = '{}'", value );
                }
            }
            case XMLStreamConstants.END_ELEMENT -> LOGGER.trace( "End element = '{}'", reader.getLocalName() );
            case XMLStreamConstants.COMMENT ->
            {
                if ( reader.hasText() )
                {
                    LOGGER.trace( reader.getText() );
                }
            }
            default -> throw new IOException( "The event: '" +
                                              reader.getEventType()
                                              +
                                              "' is not parsed by default." );
        }
    }
}
