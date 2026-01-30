package wres.reading;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.EvaluationDeclaration;
import wres.reading.csv.CsvReader;
import wres.reading.datacard.DatacardReader;
import wres.reading.fews.PublishedInterfaceXmlReader;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.reading.netcdf.nwm.NwmGridReader;
import wres.reading.netcdf.nwm.NwmVectorReader;
import wres.reading.nwis.ogc.NwisReader;
import wres.reading.nwis.ogc.response.NwisResponseReader;
import wres.reading.nwis.iv.response.NwisIvResponseReader;
import wres.reading.nwis.iv.NwisIvReader;
import wres.reading.wrds.ahps.WrdsAhpsReader;
import wres.reading.wrds.hefs.WrdsHefsJsonReader;
import wres.reading.wrds.hefs.WrdsHefsReader;
import wres.reading.wrds.nwm.WrdsNwmReader;
import wres.reading.wrds.ahps.WrdsAhpsJsonReader;
import wres.reading.wrds.nwm.WrdsNwmJsonReader;
import wres.system.SystemSettings;

/**
 * <p>Factory class that creates time-series readers for a {@link DataSource}.
 *
 * <p>TODO: When gridded reading is par with vectors, remove the features cache from this class: #51232.
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

    /** NWIS IV response reader. */
    private static final NwisIvResponseReader NWIS_IV_RESPONSE_READER = NwisIvResponseReader.of();

    /** NWIS DV response reader. */
    private static final NwisResponseReader NWIS_DV_RESPONSE_READER = NwisResponseReader.of();

    /** WRDS AHPS JSON reader. */
    private static final WrdsAhpsJsonReader WRDS_AHPS_JSON_READER = WrdsAhpsJsonReader.of();

    /** WRDS NWM JSON reader. */
    private static final WrdsNwmJsonReader WRDS_NWM_JSON_READER = WrdsNwmJsonReader.of();

    /** WRDS HEFS JSON reader. */
    private static final WrdsHefsJsonReader WRDS_HEFS_JSON_READER = WrdsHefsJsonReader.of();

    /** Pair declaration, which is used to build some readers. */
    private final EvaluationDeclaration declaration;

    /** The system settings. */
    private final SystemSettings systemSettings;

    /** The gridded features cache, required for gridded reading. */
    private final GriddedFeatures.Builder features;

    /**
     * @param declaration the pair declaration, which is used to assist in chunking requests from web services, optional
     * @param systemSettings the system settings, which are required by some readers to instantiate thread pools
     * @param features the gridded features cache used to read gridded data
     * @return an instance
     */

    public static TimeSeriesReaderFactory of( EvaluationDeclaration declaration,
                                              SystemSettings systemSettings,
                                              GriddedFeatures.Builder features )
    {
        return new TimeSeriesReaderFactory( declaration, systemSettings, features );
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

        switch ( dataSource.disposition() )
        {
            case CSV_WRES ->
            {
                return CSV_READER;
            }
            case DATACARD ->
            {
                return DATACARD_READER;
            }
            case XML_FI_TIMESERIES, XML_PI_TIMESERIES ->
            {
                return PIXML_READER;
            }
            case JSON_WATERML ->
            {
                // A WaterML source from USGS NWIS?
                if ( ReaderUtilities.isNwisIvSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from USGS NWIS.",
                                  dataSource );
                    TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                              declaration,
                                                                              dataSource );

                    return NwisIvReader.of( this.getDeclaration(), this.systemSettings, timeChunker );
                }
                // A reader for USGS-formatted WaterML, but not from a NWIS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as USGS-formatted WaterML from a "
                              + "source other than NWIS.",
                              dataSource );
                return NWIS_IV_RESPONSE_READER;
            }
            case GEOJSON ->
            {
                // A GeoJSON source from USGS NWIS?
                if ( ReaderUtilities.isNwisOgcSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from USGS NWIS.",
                                  dataSource );

                    // Set the default chunking strategy
                    TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                              declaration,
                                                                              dataSource );

                    return NwisReader.of( this.getDeclaration(), this.systemSettings, timeChunker );
                }
                // A reader for USGS-formatted GeoJSON, but not from a NWIS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as USGS-formatted GeoJSON from a "
                              + "source other than NWIS.",
                              dataSource );
                return NWIS_DV_RESPONSE_READER;
            }
            case JSON_WRDS_AHPS ->
            {
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    // Adopt a time-chunking strategy that depends on data type
                    TimeChunker timeChunker;
                    if ( ReaderUtilities.isWrdsObservedSource( dataSource ) )
                    {
                        timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.YEAR_RANGES,
                                                                      declaration,
                                                                      dataSource );
                    }
                    else
                    {
                        timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.SIMPLE_RANGE,
                                                                      declaration,
                                                                      dataSource );
                    }

                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from WRDS.",
                                  dataSource );
                    return WrdsAhpsReader.of( this.getDeclaration(), this.systemSettings, timeChunker );
                }
                // A reader for WRDS-formatted JSON from AHPS, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "AHPS time-series from a source other than WRDS.",
                              dataSource );
                return WRDS_AHPS_JSON_READER;
            }
            case JSON_WRDS_NWM ->
            {
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    return WrdsNwmReader.of( this.getDeclaration(), this.systemSettings );
                }
                // A reader for WRDS-formatted JSON from the NWM, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "NWM time-series from a source other than WRDS.",
                              dataSource );
                return WRDS_NWM_JSON_READER;
            }
            case JSON_WRDS_HEFS ->
            {
                // A web source? If so, assume a WRDS instance.
                if ( ReaderUtilities.isWebSource( dataSource ) )
                {
                    LOGGER.debug( "Discovered a data source {}, which was identified as originating from WRDS.",
                                  dataSource );

                    TimeChunker timeChunker = ReaderUtilities.getTimeChunker( TimeChunker.ChunkingStrategy.SIMPLE_RANGE,
                                                                              declaration,
                                                                              dataSource );

                    return WrdsHefsReader.of( this.getDeclaration(), this.systemSettings, timeChunker );
                }
                // A reader for WRDS-formatted JSON from HEFS, but not from a WRDS instance
                LOGGER.debug( "Discovered a data source {}, which was identified as WRDS-formatted JSON containing "
                              + "HEFS time-series from a source other than WRDS.",
                              dataSource );
                return WRDS_HEFS_JSON_READER;
            }
            case TARBALL ->
            {
                return TarredReader.of( this, this.systemSettings );
            }
            case GZIP ->
            {
                return ZippedReader.of( this );
            }
            case NETCDF_GRIDDED ->
            {
                return NwmGridReader.of( this.getDeclaration(), this.features );
            }
            case NETCDF_VECTOR ->
            {
                return NwmVectorReader.of( this.getDeclaration() );
            }
            case UNKNOWN ->
                    throw new IllegalArgumentException( "The data source could not be read because the format of the "
                                                        + "content is UNKNOWN. This may have occurred because the source "
                                                        + "is corrupt or because the content type is unrecognized or "
                                                        + "unsupported: "
                                                        + dataSource
                                                        + "." );
            default -> throw new IllegalArgumentException(
                    "There is no reader implementation available for the prescribed "
                    + "data source: "
                    + dataSource
                    + "." );
        }
    }

    /**
     * @return the declaration
     */
    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
    }

    /**
     * Create an instance.
     * @param declaration the declaration, which is used to assist in reading some sources
     * @param systemSettings the system settings, which are required by some readers to instantiate thread pools
     * @param features the gridded features cache, which is used to read gridded data
     */

    private TimeSeriesReaderFactory( EvaluationDeclaration declaration,
                                     SystemSettings systemSettings,
                                     GriddedFeatures.Builder features )
    {
        // Defer validation until it is established as required
        this.declaration = declaration;
        this.systemSettings = systemSettings;
        this.features = features;

        if ( LOGGER.isWarnEnabled() && Objects.isNull( declaration ) )
        {
            LOGGER.warn( "Creating a reader factory with missing declaration. If a reader is subsequently "
                         + "requested that depends on this declaration, you can expect an error at that time." );
        }
    }

}
