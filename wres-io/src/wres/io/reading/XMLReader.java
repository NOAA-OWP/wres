package wres.io.reading;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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

import wres.util.Strings;

/**
 * @author Tubbs
 *
 */
public abstract class XMLReader
{
    private final String filename;
    private final boolean findOnClasspath;
    private final InputStream inputStream;
    private XMLInputFactory factory = null;

    /**
     * 
     * @param filename the file name
     */
    protected XMLReader( String filename )
    {
        this( filename, false );
    }

    protected XMLReader( String fileName, InputStream inputStream )
    {
        this.findOnClasspath = false;
        this.filename = fileName;
        this.inputStream = inputStream;
    }

    protected XMLReader( String filename, boolean findOnClasspath )
    {
        this.filename = filename;
        this.findOnClasspath = findOnClasspath;
        this.inputStream = null;

        this.getLogger().trace( "Created XMLReader for file: {} findOnClasspath={}",
                                filename,
                                findOnClasspath );
    }

    protected String getFilename()
    {
        return filename;
    }

    private InputStream getFile() throws IOException
    {
        InputStream stream = ClassLoader.getSystemResourceAsStream( filename );
        if ( filename.endsWith( ".gz" ) )
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
        XMLStreamReader reader = null;

        if ( factory == null )
        {
            factory = XMLInputFactory.newFactory();
        }

        //Return the system resource reader if its found as a system resource.
        try
        {
            if ( findOnClasspath )
            {
                return factory.createXMLStreamReader( getFile() );
            }
        }
        catch ( XMLStreamException | IOException error )
        {
            this.getLogger().debug( "An XMLStreamReader could not be created "
                                    + "by looking for the source on the class "
                                    + "path. A reader will need to be created "
                                    + "by evaluating the file name or given "
                                    + "input stream." );
        }

        //If its not a system resource, or the resource cannot be read, then find on the file system.
        if ( this.inputStream != null )
        {
            reader = factory.createXMLStreamReader( inputStream );
        }
        else if ( this.filename != null )
        {
            if ( getFilename().endsWith( ".gz" ) )
            {
                InputStream fileStream = new FileInputStream( new File( getFilename() ) );
                GZIPInputStream gzipStream = new GZIPInputStream( fileStream );
                reader = factory.createXMLStreamReader( gzipStream );
            }
            else
            {
                reader = factory.createXMLStreamReader( new FileReader( getFilename() ) );
            }
        }

        if (reader == null)
        {
            throw new IOException( "No XMLReader could be created; XML could not be found." );
        }

        return reader;
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
