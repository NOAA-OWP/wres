package wres.io.reading.wrds;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.WrdsNwmReader;
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
public class WRDSSource extends BasicSource
{
    private final TimeSeriesIngester timeSeriesIngester;
    private final SystemSettings systemSettings;

    public WRDSSource( TimeSeriesIngester timeSeriesIngester,
                       ProjectConfig projectConfig,
                       DataSource dataSource,
                       SystemSettings systemSettings )
    {
        super( projectConfig, dataSource );
        this.timeSeriesIngester = timeSeriesIngester;
        this.systemSettings = systemSettings;
    }

    @Override
    public List<IngestResult> save() throws IOException
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
                                                      this.getProjectConfig(),
                                                      this.getDataSource(),
                                                      this.getSystemSettings() );
            return reader.call();
        }
        else
        {
            ReadValueManager reader =
                    this.createReadValueManager( this.getTimeSeriesIngester(),
                                                 this.getDataSource() );
            return reader.save();
        }
    }

    ReadValueManager createReadValueManager( TimeSeriesIngester timeSeriesIngester,
                                             DataSource dataSource )
    {
        return new ReadValueManager( timeSeriesIngester,
                                     dataSource );
    }
    
    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }
    
    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }
    
}
