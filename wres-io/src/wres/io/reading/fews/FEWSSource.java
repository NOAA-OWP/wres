package wres.io.reading.fews;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import wres.config.generated.ProjectConfig;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;

/**
 * @author Christopher Tubbs
 * @author James Brown
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource extends BasicSource
{
    /** The time-series ingester. */
    private final TimeSeriesIngester timeSeriesIngester;

	/**
     * @param timeSeriesIngester the time-series ingester
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
     * @throws NullPointerException if any input is null
	 */
    public FEWSSource( TimeSeriesIngester timeSeriesIngester,
                       ProjectConfig projectConfig,
                       DataSource dataSource )
    {
        super( projectConfig, dataSource );
        
        Objects.requireNonNull( timeSeriesIngester );
        
        this.timeSeriesIngester = timeSeriesIngester;
	}

    @Override
    public List<IngestResult> save() throws IOException
    {
        PIXMLReader sourceReader = new PIXMLReader( this.getTimeSeriesIngester(),
                                                    this.getDataSource() );
        sourceReader.parse();
        return sourceReader.getIngestResults();
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }
}
