package wres.reading;

import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.ThresholdSource;
import wres.reading.csv.CsvThresholdReader;
import wres.reading.wrds.thresholds.WrdsThresholdReader;

/**
 * A factory class for creating threshold readers.
 * @author James Brown
 */
public class ThresholdReaderFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ThresholdReaderFactory.class );

    /**
     * Creates a thresholds reader from the supplied source.
     *
     * @param source the threshold source
     * @return a threshold reader
     * @throws NullPointerException if the source is null
     * @throws IllegalArgumentException if there is no known reader for the source
     */

    public static ThresholdReader getReader( ThresholdSource source )
    {
        Objects.requireNonNull( source );

        URI sourceUri = source.uri();

        if ( Objects.isNull( sourceUri ) )
        {
            throw new IllegalArgumentException( "The URI was missing from the threshold source, which is not allowed. "
                                                + "Add a URI to each threshold source in order to read thresholds from "
                                                + "that source." );
        }

        // Web source: only WRDS supported at present. If other web sources are supported in future, an interface hint
        // will be required, analogous to the hint used for data sources more generally
        if ( ReaderUtilities.isWebSource( sourceUri ) )
        {
            LOGGER.debug( "When inspecting the threshold source {}, detected a web-like URI, which is assumed to "
                          + "correspond to the WRDS threshold service.", sourceUri );

            return WrdsThresholdReader.of();
        }

        // File source: detect content type
        DataSource.DataDisposition type;
        try
        {
            type = DataSource.detectFormat( sourceUri );
        }
        catch ( ReadingException e )
        {
            throw new ThresholdReadingException( "Encountered an error while trying to read thresholds from "
                                                 + sourceUri, e );
        }

        // WRDS JSON thresholds
        if ( type == DataSource.DataDisposition.JSON_WRDS_THRESHOLDS )
        {
            return WrdsThresholdReader.of();
        }
        // CSV thresholds
        else if ( type == DataSource.DataDisposition.CSV_WRES_THRESHOLDS )
        {
            return CsvThresholdReader.of();
        }
        // Unknown threshold format
        else
        {
            throw new IllegalArgumentException( "The threshold source contained in "
                                                + sourceUri
                                                + " is not a supported format for "
                                                + "thresholds. The supported formats include WRDS JSON and CSV." );
        }
    }

    /**
     * Do not construct.
     */
    private ThresholdReaderFactory()
    {
    }
}
