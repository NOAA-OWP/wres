package wres.io.reading.wrds;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.ReadException;
import wres.io.reading.Source;
import wres.io.reading.wrds.nwm.WrdsNwmReader;
import wres.system.SystemSettings;

/**
 * Ingests JSON data from the WRDS schema from either the service or JSON files on the file system
 * <p>
 *     The project config will be needed to be modified to let the user indicate the format + the
 *     service where the data will come from. Currently, if you try to get the data from the service,
 *     it will attempt to find the service on the file system in the sourceloader and fail.
 * </p>
 * <p>
 *     Additional logic will be needed to fill service parameters, such as issued time ranges,
 *     valid time ranges, and location IDs.
 * </p>
 */
public class WRDSSource implements Source
{
    private final TimeSeriesIngester timeSeriesIngester;
    private final SystemSettings systemSettings;
    private final DataSource dataSource;

    public WRDSSource( TimeSeriesIngester timeSeriesIngester,
                       ProjectConfig projectConfig,
                       DataSource dataSource,
                       SystemSettings systemSettings )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        
        this.timeSeriesIngester = timeSeriesIngester;
        this.systemSettings = systemSettings;
        this.dataSource = dataSource;
    }

    @Override
    public List<IngestResult> save()
    {
        InterfaceShortHand interfaceShortHand = this.getDataSource()
                                                    .getSource()
                                                    .getInterface();

        // Allow detected content or declared interface: #106060
        if ( this.getDataSource().getDisposition() == DataDisposition.JSON_WRDS_NWM
             || ( Objects.nonNull( interfaceShortHand )
                  && interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM ) ) )
        {
            WrdsNwmReader reader = new WrdsNwmReader( this.getTimeSeriesIngester(),
                                                      this.getDataSource(),
                                                      this.getSystemSettings() );
            return reader.call();
        }
        else
        {
            ReadValueManager reader =
                    this.createReadValueManager( this.getTimeSeriesIngester(),
                                                 this.getDataSource() );
            try
            {
                return reader.save();
            }
            catch ( IOException e )
            {
                throw new ReadException( "Failed to read a WRDS source.", e );
            }
        }
    }

    ReadValueManager createReadValueManager( TimeSeriesIngester timeSeriesIngester,
                                             DataSource dataSource )
    {
        return new ReadValueManager( timeSeriesIngester,
                                     dataSource );
    }
    
    /**
     * @return the time-series ingester
     */
    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    /**
     * @return the data source
     */
    private DataSource getDataSource()
    {
        return this.dataSource;
    }
    
    /**
     * @return the system settings
     */
    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }
    
}
