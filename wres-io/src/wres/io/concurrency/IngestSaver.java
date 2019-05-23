package wres.io.concurrency;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.reading.ReaderFactory;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;

/**
 * Saves the forecast at the indicated path asynchronously
 *
 * @author Christopher Tubbs
 */
public class IngestSaver extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( IngestSaver.class );

    public static IngestBuilder createTask()
    {
        return new IngestBuilder(  );
    }

    public enum HashStatus
    {
        ALREADY_HASHED,
        NOT_YET_HASHED
    }

    public static class IngestBuilder
    {
        private final ProjectConfig projectConfig;
        private final DataSource dataSource;
        private final String hash;
        private final HashStatus hashStatus;
        private final boolean monitorProgress;
        private final DatabaseLockManager lockManager;

        private IngestBuilder()
        {
            this.dataSource = null;
            this.projectConfig = null;
            this.hash = null;
            this.hashStatus = null;
            this.monitorProgress = false;
            this.lockManager = null;
        }


        public IngestBuilder withProject(final ProjectConfig projectConfig)
        {
            return new IngestBuilder(
                    projectConfig,
                    this.dataSource,
                    this.hash,
                    this.hashStatus,
                    this.monitorProgress,
                    this.lockManager
            );
        }

        public IngestBuilder withDataSource( DataSource dataSource )
        {
            return new IngestBuilder(
                    this.projectConfig,
                    dataSource,
                    this.hash,
                    this.hashStatus,
                    this.monitorProgress,
                    this.lockManager
            );
        }

        public IngestBuilder withHash(final String hash)
        {
            return new IngestBuilder(
                    this.projectConfig,
                    this.dataSource,
                    hash,
                    HashStatus.ALREADY_HASHED,
                    this.monitorProgress,
                    this.lockManager
            );
        }

        public IngestBuilder withoutHash()
        {
            return new IngestBuilder(
                    this.projectConfig,
                    this.dataSource,
                    null,
                    HashStatus.NOT_YET_HASHED,
                    this.monitorProgress,
                    this.lockManager
            );
        }

        public IngestBuilder withProgressMonitoring()
        {
            return new IngestBuilder(
                    this.projectConfig,
                    this.dataSource,
                    this.hash,
                    this.hashStatus,
                    true,
                    this.lockManager
            );
        }

        public IngestBuilder withLockManager( DatabaseLockManager lockManager )
        {
            return new IngestBuilder(
                    this.projectConfig,
                    this.dataSource,
                    this.hash,
                    this.hashStatus,
                    this.monitorProgress,
                    lockManager
            );
        }

        private IngestBuilder( ProjectConfig projectConfig,
                               DataSource dataSource,
                               String hash,
                               HashStatus hashStatus,
                               boolean monitorProgress,
                               DatabaseLockManager lockManager )
        {
            this.projectConfig = projectConfig;
            this.dataSource = dataSource;
            this.hash = hash;
            this.hashStatus = hashStatus;
            this.monitorProgress = monitorProgress;
            this.lockManager = lockManager;
        }

        private void validate()
        {
            if (this.projectConfig == null)
            {
                throw new IllegalArgumentException( "Data cannot be ingested from a nonexistent project." );
            }

            if ( this.dataSource == null )
            {
                throw new IllegalArgumentException(
                        "Data cannot be ingested from a nonexistent data source configuration"
                );
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

        public IngestSaver build()
        {
            validate();
            IngestSaver saver = new IngestSaver(
                    this.projectConfig,
                    this.dataSource,
                    this.hash,
                    this.hashStatus,
                    this.lockManager
            );

            if (this.monitorProgress)
            {
                saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
                saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            }

            return saver;
        }
    }

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final String hash;
    private final HashStatus hashStatus;
    private final DatabaseLockManager lockManager;

    private IngestSaver( ProjectConfig projectConfig,
                         DataSource dataSource,
                         String hash,
                         HashStatus hashStatus,
                         DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.hash = hash;
        this.hashStatus = hashStatus;
        this.lockManager = lockManager;
    }


    @Override
    public List<IngestResult> execute() throws IOException
    {
        BasicSource source = ReaderFactory.getReader( this.projectConfig,
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
