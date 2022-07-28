package wres.io.reading.fews;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.Source;

/**
 * @author Christopher Tubbs
 * @author James Brown
 * Interprets a FEWS (PIXML) source into either forecast or observation data and stores them in the database
 */
public class FEWSSource implements Source
{
    /** The time-series ingester. */
    private final TimeSeriesIngester timeSeriesIngester;
    private final DataSource dataSource;

	/**
     * @param timeSeriesIngester the time-series ingester
     * @param dataSource the data source information
     * @throws NullPointerException if any input is null
	 */
    public FEWSSource( TimeSeriesIngester timeSeriesIngester,
                       DataSource dataSource )
    {
        Objects.requireNonNull( timeSeriesIngester );
        
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
	}

    @Override
    public List<IngestResult> save()
    {
        try
        {
            PIXMLReader sourceReader = new PIXMLReader( this.getTimeSeriesIngester(),
                                                        this.getDataSource() );
            sourceReader.parse();
            return sourceReader.getIngestResults();
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a PI-XML data source.", e );
        }
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

}
