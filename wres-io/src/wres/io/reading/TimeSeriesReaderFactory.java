package wres.io.reading;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.PairConfig;
import wres.io.database.caching.GriddedFeatures;
import wres.io.reading.commaseparated.CsvReader;
import wres.io.reading.datacard.DatacardReader;
import wres.io.reading.fews.PublishedInterfaceXmlReader;
import wres.io.reading.netcdf.nwm.NwmGridReader;
import wres.io.reading.netcdf.nwm.NwmVectorReader;
import wres.io.reading.waterml.WatermlReader;
import wres.io.reading.web.NwisReader;
import wres.io.reading.web.WrdsAhpsReader;
import wres.io.reading.web.WrdsNwmReader;
import wres.io.reading.wrds.WrdsAhpsJsonReader;
import wres.io.reading.wrds.nwm.WrdsNwmJsonReader;
import wres.system.SystemSettings;

/**
 * Factory class that creates time-series readers for a {@link DataSource}.
 * 
 * TODO: When gridded reading is par with vectors, remove the features cache from this class: #51232.
 * 
 * @author James Brown
 */

public class TimeSeriesReaderFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesReaderFactory.class );

    /** CSV reader. */
    private static final CsvReader CSV_READER = CsvReader.of();

    /** Datacard reader. */
    private static final DatacardReader DATACARD_READER = DatacardReader.of();

    /** PI-XML and FastInfoset PI-XML reader. */
    private static final PublishedInterfaceXmlReader PIXML_READER = PublishedInterfaceXmlReader.of();

    /** WaterML reader. */
    private static final WatermlReader WATERML_READER = WatermlReader.of();

    /** WRDS AHPS JSON reader. */
    private static final WrdsAhpsJsonReader WRDS_AHPS_JSON_READER = WrdsAhpsJsonReader.of();

    /** WRDS NWM JSON reader. */
    private static final WrdsNwmJsonReader WRDS_NWM_JSON_READER = WrdsNwmJsonReader.of();

    /** Pair declaration, which is used to build some readers. */
    private final PairConfig pairConfig;

    /** The system settings. */
    private final SystemSettings systemSettings;

    /** The gridded features cache, required for gridded reading. */
    private final GriddedFeatures.Builder features;

    /**
     * @param pairConfig the pair declaration, which is used to assist in chunking requests from web services, optional
     * @param systemSettings the system settings, which are required by some readers to instantiate thread pools
     * @param features the gridded features cache used to read gridded data
     * @return an instance
     */

    public static TimeSeriesReaderFactory of( PairConfig pairConfig,
                                              SystemSettings systemSettings,
                                              GriddedFeatures.Builder features )
    {
        return new TimeSeriesReaderFactory( pairConfig, systemSettings, features );
    }

    /**
     * Returns a concrete reader for the prescribed data source.
     * 
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
                return CSV_READER;
            case DATACARD:
                return DATACARD_READER;
            case XML_FI_TIMESERIES:
            case XML_PI_TIMESERIES:
                return PIXML_READER;
            case JSON_WATERML:
                // A WaterML source from USGS NWIS?
                if ( ReaderUtilities.isUsgsSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from USGS NWIS.",
                                  dataSource );
                    return NwisReader.of( this.pairConfig, this.systemSettings );
                }
                // A reader for USGS-formatted WaterML, but not from a NWIS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as USGS-formatted WaterML from a "
                              + "source other than NWIS.",
                              dataSource );
                return WATERML_READER;
            case JSON_WRDS_AHPS:
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from WRDS.",
                                  dataSource );
                    return WrdsAhpsReader.of( this.pairConfig, this.systemSettings );
                }
                // A reader for WRDS-formatted JSON from AHPS, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "AHPS time-series from a source other than WRDS.",
                              dataSource );
                return WRDS_AHPS_JSON_READER;
            case JSON_WRDS_NWM:
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    return WrdsNwmReader.of( this.pairConfig, this.systemSettings );
                }
                // A reader for WRDS-formatted JSON from the NWM, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "NWM time-series from a source other than WRDS.",
                              dataSource );
                return WRDS_NWM_JSON_READER;
            case TARBALL:
                return TarredReader.of( this, this.systemSettings );
            case GZIP:
                return ZippedReader.of( this );
            case NETCDF_GRIDDED:
                return NwmGridReader.of( this.pairConfig, this.features );
            case NETCDF_VECTOR:
                return NwmVectorReader.of( this.pairConfig );
            case UNKNOWN:
                throw new IllegalArgumentException( "The data source could not be read because the format of the "
                                                    + "content is UNKNOWN. This may have occurred because the source "
                                                    + "is corrupt or because the content type is unrecognized or "
                                                    + "unsupported: "
                                                    + dataSource
                                                    + "." );
            default:
                throw new IllegalArgumentException( "There is no reader implementation available for the prescribed "
                                                    + "data source: "
                                                    + dataSource
                                                    + "." );
        }
    }

    /**
     * Create an instance.
     * @param pairConfig the pair declaration, which is used to assist in reading some sources
     * @param systemSettings the system settings, which are required by some readers to instantiate thread pools
     * @param features the gridded features cache, which is used to read gridded data
     */

    private TimeSeriesReaderFactory( PairConfig pairConfig,
                                     SystemSettings systemSettings,
                                     GriddedFeatures.Builder features )
    {
        // Defer validation until it is established as required
        this.pairConfig = pairConfig;
        this.systemSettings = systemSettings;
        this.features = features;

        if ( LOGGER.isWarnEnabled() && Objects.isNull( pairConfig ) )
        {
            LOGGER.warn( "Creating a reader factory with missing pair declaration. If a reader is subsequently "
                         + "requested that depends on pair declaration, you can expect an error at that time." );
        }
    }

}
