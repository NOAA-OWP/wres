package wres.io.concurrency;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.reading.ReaderFactory;
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

    public static class IngestBuilder
    {
        private final URI filepath;
        private final ProjectConfig projectConfig;
        private final DataSourceConfig dataSourceConfig;
        private final DataSourceConfig.Source sourceConfig;
        private final String hash;
        private final boolean monitorProgress;
        private final boolean isRemote;

        private IngestBuilder()
        {
            this.filepath = null;
            this.projectConfig = null;
            this.dataSourceConfig = null;
            this.sourceConfig = null;
            this.hash = null;
            this.monitorProgress = false;
            this.isRemote = false;
        }

        public IngestBuilder withFilePath( final URI filepath )
        {
            return new IngestBuilder(
                    filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    this.monitorProgress,
                    this.isRemote
            );
        }

        public IngestBuilder withProject(final ProjectConfig projectConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    this.monitorProgress,
                    this.isRemote
            );
        }

        public IngestBuilder withDataSourceConfig(final DataSourceConfig dataSourceConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    this.monitorProgress,
                    this.isRemote
            );
        }

        public IngestBuilder withSourceConfig(final DataSourceConfig.Source sourceConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    sourceConfig,
                    this.hash,
                    this.monitorProgress,
                    this.isRemote
            );
        }

        public IngestBuilder withHash(final String hash)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    hash,
                    this.monitorProgress,
                    this.isRemote
            );
        }

        public IngestBuilder withProgressMonitoring()
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    true,
                    this.isRemote
            );
        }

        // The 'isRemote' indicator doesn't really drive behavior; other methods are used to determine this
        @Deprecated(forRemoval = true)
        public IngestBuilder isRemote()
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    this.monitorProgress,
                    true
            );
        }

        private IngestBuilder(
                final URI filepath,
                final ProjectConfig projectConfig,
                final DataSourceConfig dataSourceConfig,
                final DataSourceConfig.Source sourceConfig,
                final String hash,
                final boolean monitorProgress,
                final boolean isRemote)
        {
            this.filepath = filepath;
            this.projectConfig = projectConfig;
            this.dataSourceConfig = dataSourceConfig;
            this.sourceConfig = sourceConfig;
            this.hash = hash;
            this.monitorProgress = monitorProgress;
            this.isRemote = isRemote;
        }

        private void validate()
        {
            if (this.projectConfig == null)
            {
                throw new IllegalArgumentException( "Data cannot be ingested from a nonexistent project." );
            }

            if (this.dataSourceConfig == null)
            {
                throw new IllegalArgumentException(
                        "Data cannot be ingested from a nonexistent data source configuration"
                );
            }
        }

        public IngestSaver build()
        {
            validate();
            IngestSaver saver = new IngestSaver(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.hash,
                    this.isRemote
            );

            if (this.monitorProgress)
            {
                saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
                saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            }

            return saver;
        }
    }

    private final URI filepath;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final String hash;
    private final boolean isRemote;

    private IngestSaver( URI filepath,
                        ProjectConfig projectConfig,
                        DataSourceConfig dataSourceConfig,
                        DataSourceConfig.Source sourceConfig,
                        final String hash,
                         final boolean isRemote)
    {
        this.filepath = filepath;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.hash = hash;
        this.isRemote = isRemote;
    }


    @Override
    public List<IngestResult> execute() throws IOException
    {
        BasicSource source = ReaderFactory.getReader( this.projectConfig,
                                                      this.filepath );
        source.setDataSourceConfig( this.dataSourceConfig );
        source.setSourceConfig( this.sourceConfig );
        source.setHash( this.hash );
        source.setIsRemote( this.isRemote );
        return source.save();
    }


    @Override
    protected Logger getLogger()
    {
        return IngestSaver.LOGGER;
    }
}
