package wres.io.concurrency;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.fews.PIXMLReader;

/**
 * Created by ctubbs on 7/19/17.
 */
public final class ZippedPIXMLIngest extends WRESCallable<List<IngestResult>>
{
    private final DataSource dataSource;
    private final byte[] content;
    private final TimeSeriesIngester timeSeriesIngester;

    public ZippedPIXMLIngest ( TimeSeriesIngester timeSeriesIngester,
                               DataSource dataSource,
                               byte[] content )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( content );
        
        this.dataSource = dataSource;
        this.content = Arrays.copyOf( content, content.length );
        this.timeSeriesIngester = timeSeriesIngester;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    @Override
    public List<IngestResult> execute() throws IOException
    {
        try ( InputStream input = new ByteArrayInputStream( this.content ) )
        {
            PIXMLReader reader = new PIXMLReader( this.getTimeSeriesIngester(),
                                                  this.getDataSource(),
                                                  input );
            reader.parse();
            return reader.getIngestResults();
        }
    }
}
