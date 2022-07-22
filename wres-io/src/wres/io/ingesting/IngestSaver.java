package wres.io.ingesting;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESCallable;
import wres.io.data.caching.Caches;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.ReaderFactory;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * Saves the forecast at the indicated path asynchronously
 *
 * @author Christopher Tubbs
 */
public class IngestSaver extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestSaver.class );
    private final SystemSettings systemSettings;
    private final Database database;
    private final Caches caches;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final String hash;
    private final HashStatus hashStatus;
    private final DatabaseLockManager lockManager;
    private final TimeSeriesIngester timeSeriesIngester;

    public enum HashStatus
    {
        ALREADY_HASHED,
        NOT_YET_HASHED
    }

    public static class Builder
    {
        private SystemSettings systemSettings = null;
        private Database database = null;
        private Caches caches = null;
        private ProjectConfig projectConfig = null;
        private DataSource dataSource = null;
        private String hash = null;
        private HashStatus hashStatus = null;
        private boolean monitorProgress = false;
        private DatabaseLockManager lockManager = null;
        private TimeSeriesIngester timeSeriesIngester = null;

        public Builder withSystemSettings( SystemSettings systemSettings )
        {
            this.systemSettings = systemSettings;
            return this;
        }

        public Builder withDatabase( Database database )
        {
            this.database = database;
            return this;
        }

        public Builder withCaches( Caches caches )
        {
            this.caches = caches;
            return this;
        }

        public Builder withProject( final ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;
            return this;
        }

        public Builder withDataSource( DataSource dataSource )
        {
            this.dataSource = dataSource;
            return this;
        }

        public Builder withHash( final String hash )
        {
            this.hash = hash;
            return this;
        }

        public Builder withoutHash()
        {
            this.hash = null;
            this.hashStatus = HashStatus.NOT_YET_HASHED;
            return this;
        }

        public Builder withProgressMonitoring()
        {
            this.monitorProgress = true;
            return this;
        }

        public Builder withLockManager( DatabaseLockManager lockManager )
        {
            this.lockManager = lockManager;
            return this;
        }
        
        public Builder withTimeSeriesIngester( TimeSeriesIngester timeSeriesIngester )
        {
            this.timeSeriesIngester = timeSeriesIngester;
            return this;
        }

        public IngestSaver build()
        {
            return new IngestSaver( this );
        }
    }

    private IngestSaver( Builder builder )
    {
        this.systemSettings = builder.systemSettings;
        this.database = builder.database;
        this.caches = builder.caches;
        this.projectConfig = builder.projectConfig;
        this.dataSource = builder.dataSource;
        this.hash = builder.hash;
        this.hashStatus = builder.hashStatus;
        this.lockManager = builder.lockManager;
        this.timeSeriesIngester = builder.timeSeriesIngester;
        if ( builder.monitorProgress )
        {
            this.setOnRun( ProgressMonitor.onThreadStartHandler() );
            this.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
        }

        // Validate
        Objects.requireNonNull( this.systemSettings );
        Objects.requireNonNull( this.timeSeriesIngester );

        if( ! this.systemSettings.isInMemory() )
        {
            Objects.requireNonNull( this.database );
            Objects.requireNonNull( this.caches );
        }
        
        if ( this.projectConfig == null )
        {
            throw new IllegalArgumentException( "Data cannot be ingested from a nonexistent project." );
        }

        if ( this.dataSource == null )
        {
            throw new IllegalArgumentException(
                                                "Data cannot be ingested from a nonexistent data source configuration" );
        }

        // The purpose of hashStatus is to make the caller to consider
        // whether or not the hash of the data has already been computed,
        // and if so, to specify it, and if not, to not specify it. There
        // will be less ambiguity about what a null hash means. The reason
        // an enum is created instead of a boolean is that a primitive
        // boolean must be true or false, and the enum allows more relevant
        // language.

        if ( this.hashStatus == null )
        {
            throw new IllegalArgumentException(
                                                "The caller must declare whether or not the source has "
                                                + "already been hashed by specifying withHash() or by "
                                                + "specifying withoutHash()." );
        }

        // The following two are unlikely due to the way .withHash and
        // .withoutHash are exposed above, but for even more clarity these
        // are left here.
        if ( this.hashStatus == HashStatus.ALREADY_HASHED
             && ( this.hash == null || this.hash.isBlank() ) )
        {
            throw new IllegalArgumentException( "When the source has been hashed, the hash must be specified." );
        }

        if ( this.hashStatus == HashStatus.NOT_YET_HASHED
             && this.hash != null )
        {
            throw new IllegalArgumentException( "When the source has not been hashed, the hash must not be specified." );
        }

        if ( this.lockManager == null )
        {
            throw new IllegalArgumentException( "The lock manager must be specified." );
        }
    }


    @Override
    public List<IngestResult> execute() throws IOException
    {
        BasicSource source = ReaderFactory.getReader( this.timeSeriesIngester,
                                                      this.systemSettings,
                                                      this.database,
                                                      this.caches,
                                                      this.projectConfig,
                                                      this.dataSource,
                                                      this.lockManager );

        if ( this.hashStatus == HashStatus.ALREADY_HASHED )
        {
            source.setHash( this.hash );
        }

        return source.save();
    }


    @Override
    protected Logger getLogger()
    {
        return IngestSaver.LOGGER;
    }
}
