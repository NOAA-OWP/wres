package wres.io.ingesting.database;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.datamodel.DataFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.io.concurrency.Downloader;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.database.Database;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.DataSource;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * Ingests times-series metadata for gridded sources to a database. Does not copy any time-series values. TODO: remove 
 * this class when we have a unified approach to ingest. See #51232.
 * @author Christopher Tubbs
 * @author James Brown
 */
class GriddedMetadataSaver implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GriddedMetadataSaver.class );
    private static final FeatureKey GRIDDED_FEATURES_PLACEHOLDER =
            FeatureKey.of( Geometry.newBuilder()
                                   .setName( "PLACEHOLDER" )
                                   .setDescription( "A placeholder for gridded/raster netCDF data." )
                                   .build() );

    private SystemSettings systemSettings;
    private DatabaseCaches caches;
    private Database database;
    private DataSource dataSource;
    private URI fileName;
    private NetcdfFile source;
    private final String hash;

    public GriddedMetadataSaver( SystemSettings systemSettings,
                                 Database database,
                                 DatabaseCaches caches,
                                 DataSource dataSource,
                                 final String hash )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( hash );
        this.systemSettings = systemSettings;
        this.database = database;
        this.caches = caches;
        this.dataSource = dataSource;
        this.fileName = dataSource.getUri();
        this.hash = hash;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private DatabaseCaches getCaches()
    {
        return this.caches;
    }

    @Override
    public List<IngestResult> call() throws IOException, SQLException
    {
        this.ensureFileIsLocal();

        try
        {
            Instant referenceTime = NetCDF.getReferenceTime( this.getFile() );
            Duration lead = NetCDF.getLeadTime( this.getFile() );
            String variable = this.dataSource.getContext()
                                             .getVariable()
                                             .getValue();
            Variable ncVariable = this.getFile()
                                      .findVariable( variable );

            if ( ncVariable == null )
            {
                List<String> variableNames = this.getFile()
                                                 .getVariables()
                                                 .stream()
                                                 .map( Variable::getFullName )
                                                 .collect( Collectors.toList() );
                throw new PreIngestException( "Could not find variable '"
                                              + variable
                                              + "' in gridded netCDF resource '"
                                              + this.fileName
                                              + "'. Available variables: "
                                              + variableNames );
            }

            String measurementUnit = ncVariable.getUnitsString();
            Long measurementUnitId = this.getCaches()
                                         .getMeasurementUnitsCache()
                                         .getOrCreateMeasurementUnitId( measurementUnit );

            // For now represent all the features in the gridded netCDF blob
            // with a single gridded features placeholder feature in wres.Source
            Long featureId = this.getCaches()
                                 .getFeaturesCache()
                                 .getOrCreateFeatureId( GRIDDED_FEATURES_PLACEHOLDER );

            SourceDetails griddedSource = new SourceDetails();
            griddedSource.setSourcePath( this.fileName );

            Number leadNumeric = DataFactory.durationToNumericUnits( lead, TimeSeriesSlicer.LEAD_RESOLUTION );
            griddedSource.setLead( leadNumeric.intValue() );
            griddedSource.setHash( this.hash );
            griddedSource.setIsPointData( false );
            griddedSource.setMeasurementUnitId( measurementUnitId );
            griddedSource.setVariableName( variable );
            griddedSource.setFeatureId( featureId );

            Database database = this.getDatabase();
            griddedSource.save( database );

            if ( griddedSource.getId() == null )
            {
                throw new IOException( "Information about the gridded data source at " +
                                       this.fileName
                                       + " could not be ingested." );
            }

            // Save the reference datetime to wres.TimeSeriesReferenceTime as T0
            String[] referenceDatetimeRow = new String[3];
            referenceDatetimeRow[0] = Long.toString( griddedSource.getId() );
            referenceDatetimeRow[1] = referenceTime.toString();
            referenceDatetimeRow[2] = ReferenceTimeType.T0.toString();
            List<String[]> referenceDatetimeRows = new ArrayList<>( 1 );
            referenceDatetimeRows.add( referenceDatetimeRow );

            List<String> columns = List.of( "source_id",
                                            "reference_time",
                                            "reference_time_type" );
            boolean[] quotedColumns = { false, true, true };

            database.copy( "wres.TimeSeriesReferenceTime",
                           columns,
                           referenceDatetimeRows,
                           quotedColumns );

            SourceCompletedDetails completedDetails =
                    new SourceCompletedDetails( this.getDatabase(),
                                                griddedSource );
            boolean complete;

            if ( griddedSource.performedInsert() )
            {
                completedDetails.markCompleted();
                complete = true;
            }
            else
            {
                complete = completedDetails.wasCompleted();
            }

            return IngestResult.singleItemListFrom( this.dataSource,
                                                    griddedSource.getId(),
                                                    !griddedSource.performedInsert(),
                                                    !complete );
        }
        finally
        {
            try
            {
                this.closeFile();
            }
            catch ( IOException e )
            {
                // Exception on close should not affect primary outputs.
                LOGGER.warn( "Failed to close file {}.", this.fileName, e );
            }
        }
    }

    private void ensureFileIsLocal() throws IOException
    {
        Path path = Paths.get( this.fileName );

        if ( this.fileName.getScheme() != null &&
             this.fileName.getScheme().startsWith( "http" ) )
        {
            URL url = this.fileName.toURL();
            HttpURLConnection huc = (HttpURLConnection) url.openConnection();
            huc.setRequestMethod( "HEAD" );
            huc.setInstanceFollowRedirects( false );

            if ( huc.getResponseCode() == HttpURLConnection.HTTP_OK )
            {
                this.retrieveFile( path );
            }
        }
        else
        {
            LOGGER.trace( "It was determined that {} is not remote data.", this.fileName );

            if ( !Files.exists( path ) )
            {
                throw new IOException( "Gridded data could not be found at: '" + this.fileName + "'" );
            }
        }
    }

    private void retrieveFile( final Path path ) throws IOException
    {
        Integer nameCount = path.getNameCount();
        Integer firstNameIndex = 0;

        if ( nameCount > 4 )
        {
            firstNameIndex = nameCount - 4;
        }

        final URI originalPath = this.fileName;

        SystemSettings systemSettings = this.getSystemSettings();
        this.fileName = Paths.get(
                                   systemSettings.getNetCDFStorePath(),
                                   path.subpath( firstNameIndex, nameCount ).toString() )
                             .toUri();

        if ( !Paths.get( this.fileName.toString(), path.getFileName().toString() ).toFile().exists() )
        {
            Downloader downloader = new Downloader( Paths.get( this.fileName ), originalPath );
            downloader.setDisplayOutput( false );
            downloader.run();

            if ( !downloader.fileHasBeenDownloaded() )
            {
                throw new IOException( "The file at '" + originalPath + "' could not be downloaded." );
            }
        }
    }

    private NetcdfFile getFile() throws IOException
    {
        if ( this.source == null )
        {
            LOGGER.trace( "Now opening '{}'...", this.fileName );
            this.source = NetcdfFiles.open( this.fileName.toString() );
            LOGGER.trace( "'{}' has been opened for parsing.", this.fileName );
        }
        return this.source;
    }

    private void closeFile() throws IOException
    {
        if ( this.source != null )
        {
            this.source.close();
            this.source = null;
        }
    }
}
