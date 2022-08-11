package wres.io.reading;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.PairConfig;
import wres.io.reading.commaseparated.CsvReader;
import wres.io.reading.datacard.DatacardReader;
import wres.io.reading.fews.PublishedInterfaceXmlReader;
import wres.io.reading.waterml.WatermlReader;
import wres.io.reading.web.NwisReader;
import wres.io.reading.web.WrdsAhpsReader;
import wres.io.reading.web.WrdsNwmReader;
import wres.io.reading.wrds.WrdsAhpsJsonReader;
import wres.io.reading.wrds.nwm.WrdsNwmJsonReader;

/**
 * Factory class that creates time-series readers for a {@link DataSource}.
 * 
 * @author James Brown
 */

public class TimeSeriesReaderFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesReaderFactory.class );

    /** CSV reader. */
    private final CsvReader csvReader;

    /**
     * @param pairConfig the pair declaration, which is used to assist in chunking requests from web services, optional
     * @return an instance
     */

    public static TimeSeriesReaderFactory of( PairConfig pairConfig )
    {
        return new TimeSeriesReaderFactory( pairConfig );
    }

    /**
     * Returns a concrete reader for the prescribed data source.
     * 
     * @see #hasTimeSeriesReader(DataSource)
     * @param dataSource the data source, required
     * @return a reader
     * @throws NullPointerException if the data source is null
     * @throws IllegalArgumentException if there is no {@link TimeSeriesReader} for the dataSource
     */

    public TimeSeriesReader getReader( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        switch ( dataSource.getDisposition() )
        {
            case CSV_WRES:
                return this.csvReader;
            case DATACARD:
                return DatacardReader.of();
            case XML_FI_TIMESERIES:
            case XML_PI_TIMESERIES:
                return PublishedInterfaceXmlReader.of();
            case JSON_WATERML:
                // A WaterML source from USGS NWIS?
                if ( ReaderUtilities.isUsgsSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from USGS NWIS.",
                                  dataSource );
                    return NwisReader.of();
                }
                // A reader for USGS-formatted WaterML, but not from a NWIS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as USGS-formatted WaterML from a "
                              + "source other than NWIS.",
                              dataSource );
                return WatermlReader.of();
            case JSON_WRDS_AHPS:
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from WRDS.",
                                  dataSource );
                    return WrdsAhpsReader.of();
                }
                // A reader for WRDS-formatted JSON from AHPS, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "AHPS time-series from a source other than WRDS.",
                              dataSource );
                return WrdsAhpsJsonReader.of();
            case JSON_WRDS_NWM:
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    return WrdsNwmReader.of();
                }
                // A reader for WRDS-formatted JSON from the NWM, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "NWM time-series from a source other than WRDS.",
                              dataSource );
                return WrdsNwmJsonReader.of();
            case TARBALL:
                return TarredReader.of( this );
            case GZIP:
                return ZippedReader.of( this );
            default:
                throw new IllegalArgumentException( "There is no reader implementation available for the prescribed "
                                                    + "data source disposition: "
                                                    + dataSource
                                                    + "." );
        }
    }

    /**
     * Helper that identifies whether the reader associated with the data source implements the 
     * {@link TimeSeriesReader}.
     * 
     * TODO: remove this helper when all readers implement {@link TimeSeriesReader}.
     * 
     * @param dataSource the data source to test
     * @return true if the reader implements {@link TimeSeriesReader}, false for a legacy {@link Source}.
     * @throws NullPointerException if the data source is null
     */

    public static boolean hasTimeSeriesReader( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        switch ( dataSource.getDisposition() )
        {
            case CSV_WRES:
            case DATACARD:
            case XML_FI_TIMESERIES:
            case XML_PI_TIMESERIES:
            case JSON_WATERML:
            case JSON_WRDS_AHPS:
            case JSON_WRDS_NWM:
            case TARBALL:
            case GZIP:
                return true;
            default:
                return false;
        }
    }

    /**
     * Create an instance.
     * @param pairConfig the pair declaration, which is used to assist in chunking requests from web services
     */

    private TimeSeriesReaderFactory( PairConfig pairConfig )
    {
        this.csvReader = CsvReader.of();
    }

}
