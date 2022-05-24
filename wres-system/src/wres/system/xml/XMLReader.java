package wres.system.xml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.util.Objects;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;

import com.sun.xml.fastinfoset.stax.StAXDocumentParser;  //NOSONAR

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
    private static final XMLInputFactory DEFAULT_FACTORY = XMLInputFactory.newFactory();

    private final URI filename;
    private final InputStream inputStream;
    private final XMLInputFactory factory;
    private final boolean fastInfoset;

    /**
     * Convenience constructor - finds in either the classpath or filesystem.
     * @param filename the file name to look for on the classpath or filesystem.
     * @throws IOException when the file cannot be found.
     */
    protected XMLReader( URI filename )
            throws IOException
    {
        this( filename, null );
    }
    
    /**
     * Convenience constructor - finds in either the classpath or filesystem
     * @param filename the file name to look for on the classpath or filesystem
     * @param fastInfoset is true if the file is fast-infoset encoded
     * @throws IOException when the file cannot be found.
     */
    protected XMLReader( URI filename, boolean fastInfoset )
            throws IOException
    {
        this( filename, null, fastInfoset );
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

    private XMLStreamReader createReader() throws XMLStreamException
    {
        if( this.isFastInfoset() )
        {
            return new StAXDocumentParser( this.inputStream );
        }
        else 
        {
            return this.factory.createXMLStreamReader( this.inputStream );
        }
    }

    public String getRawXML() throws XMLStreamException, TransformerException
    {
        XMLStreamReader reader = createReader();
        
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        // Prohibit the use of all protocols by external entities:
        transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        Transformer transformer = transformerFactory.newTransformer();
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
                this.getLogger().trace( "Start element = '{}'", reader.getLocalName() );
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
                this.getLogger().trace( "End element = '{}'", reader.getLocalName() );
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
