package wres.io.concurrency;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
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
        private final String filepath;
        private final ProjectConfig projectConfig;
        private final DataSourceConfig dataSourceConfig;
        private final DataSourceConfig.Source sourceConfig;
        private final List<Feature> specifiedFeatures;
        private final String hash;
        private final boolean monitorProgress;

        private IngestBuilder()
        {
            this.filepath = null;
            this.projectConfig = null;
            this.dataSourceConfig = null;
            this.sourceConfig = null;
            this.specifiedFeatures = null;
            this.hash = null;
            this.monitorProgress = false;
        }

        public IngestBuilder withFilePath(final String filepath)
        {
            return new IngestBuilder(
                    filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.specifiedFeatures,
                    this.hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withProject(final ProjectConfig projectConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.specifiedFeatures,
                    this.hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withDataSourceConfig(final DataSourceConfig dataSourceConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    dataSourceConfig,
                    this.sourceConfig,
                    this.specifiedFeatures,
                    this.hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withSourceConfig(final DataSourceConfig.Source sourceConfig)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    sourceConfig,
                    this.specifiedFeatures,
                    this.hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withFeatures(final List<Feature> specifiedFeatures)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    specifiedFeatures,
                    this.hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withHash(final String hash)
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.specifiedFeatures,
                    hash,
                    this.monitorProgress
            );
        }

        public IngestBuilder withProgressMonitoring()
        {
            return new IngestBuilder(
                    this.filepath,
                    this.projectConfig,
                    this.dataSourceConfig,
                    this.sourceConfig,
                    this.specifiedFeatures,
                    this.hash,
                    true
            );
        }

        private IngestBuilder(
                final String filepath,
                final ProjectConfig projectConfig,
                final DataSourceConfig dataSourceConfig,
                final DataSourceConfig.Source sourceConfig,
                final List<Feature> specifiedFeatures,
                final String hash,
                final boolean monitorProgress)
        {
            this.filepath = filepath;
            this.projectConfig = projectConfig;
            this.dataSourceConfig = dataSourceConfig;
            this.sourceConfig = sourceConfig;
            this.specifiedFeatures = specifiedFeatures;
            this.hash = hash;
            this.monitorProgress = monitorProgress;
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
                    this.specifiedFeatures,
                    this.hash
            );

            if (this.monitorProgress)
            {
                saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
                saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            }

            return saver;
        }
    }

    private final String filepath;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final List<Feature> specifiedFeatures;
    private final String hash;

    private IngestSaver( String filepath,
                        ProjectConfig projectConfig,
                        DataSourceConfig dataSourceConfig,
                        DataSourceConfig.Source sourceConfig,
                        List<Feature> specifiedFeatures,
                        final String hash)
    {
        this.filepath = filepath;
        this.projectConfig = projectConfig;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.specifiedFeatures = specifiedFeatures;
        this.hash = hash;
    }


    @Override
    public List<IngestResult> execute() throws IOException
    {
        BasicSource source = ReaderFactory.getReader( this.projectConfig,
                                                      this.filepath );
        source.setDataSourceConfig( this.dataSourceConfig );
        source.setSourceConfig( this.sourceConfig );
        source.setSpecifiedFeatures( this.specifiedFeatures );
        source.setHash( this.hash );
        return source.save();
    }


    @Override
    protected Logger getLogger()
    {
        return IngestSaver.LOGGER;
    }
}
