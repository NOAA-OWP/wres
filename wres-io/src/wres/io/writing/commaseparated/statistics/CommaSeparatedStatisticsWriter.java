package wres.io.writing.commaseparated.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MissingValues;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;
import wres.util.TimeHelper;

/**
 * Helps write statistics as Comma Separated Values (CSV).
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
abstract class CommaSeparatedStatisticsWriter
{

    private static final Logger LOGGER = LoggerFactory.getLogger( CommaSeparatedStatisticsWriter.class );

    /**
     * Delimiter for the header.
     */

    static final String HEADER_DELIMITER = " ";

    /**
     * Earliest possible time window to index the header.
     */

    static final TimeWindowOuter HEADER_INDEX = TimeWindowOuter.of( Instant.MIN,
                                                                    Instant.MIN,
                                                                    Duration.ofSeconds( Long.MIN_VALUE ) );

    /**
     * Resolution for writing duration outputs.
     */

    private final ChronoUnit durationUnits;

    /**
     * The project configuration to write.
     */

    private final ProjectConfig projectConfig;

    /**
     * The directory to write to.
     */

    private final Path outputDirectory;

    /**
     * Returns the duration units for writing lead durations.
     *
     * @return the duration units
     */

    ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }

    /**
     * Returns the project declaration
     *
     * @return the project declaration
     */

    ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * @return the directory to write to
     */

    Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

    /**
     * Writes the raw tabular output to file.
     *
     * @param rows the tabular data to write (non null, not empty!)
     * @param outputPath the path to which the file should be written
     * @return the path actually written
     * @throws IOException if the output cannot be written
     * @throws IllegalArgumentException when any arg is null
     * @throws IllegalArgumentException when rows is empty
     */

    static Path writeTabularOutputToFile( List<RowCompareByLeft> rows,
                                          Path outputPath )
            throws IOException
    {
        Objects.requireNonNull( rows );
        Objects.requireNonNull( outputPath );

        if ( rows.isEmpty() )
        {
            throw new IllegalArgumentException( "Rows to write to must not be empty!" );
        }

        // Sort the rows before writing them
        Collections.sort( rows );

        // Append a file extension to the path
        Path extendedPath = outputPath.resolveSibling( outputPath.getFileName() + ".csv" );

        // Write if the path has not already been written
        if ( CommaSeparatedStatisticsWriter.validatePath( extendedPath ) )
        {

            try ( BufferedWriter w = Files.newBufferedWriter( extendedPath,
                                                              StandardCharsets.UTF_8,
                                                              StandardOpenOption.CREATE,
                                                              StandardOpenOption.TRUNCATE_EXISTING ) )
            {
                for ( RowCompareByLeft row : rows )
                {
                    w.write( row.getRight().toString() );
                    w.write( System.lineSeparator() );
                }
            }
            catch ( IOException e )
            {
                // Clean up, to allow recovery. See #83816
                CommaSeparatedStatisticsWriter.deletePath( extendedPath );
                throw new CommaSeparatedWriteException( "Encountered an error while writing " + extendedPath, e );
            }
        }

        return extendedPath;
    }

    /**
     * Validates that the file object represented by the path does not already exist.
     * 
     * @throws CommaSeparatedWriteException if the path exists
     * @return true if the path is valid to write, false if it exists and is, therefore, invalid
     */

    private static boolean validatePath( Path path )
    {
        File file = path.toFile();

        boolean fileExists = file.exists();
        
        // #81735-173 and #86077
        if ( fileExists && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Cannot write file {} because it already exists. This may occur when retrying several format "
                         + "writers of which only some failed previously, but is otherwise unexpected behavior that "
                         + "may indicate an error in format writing. The file has been retained and not modified.",
                         file );
        }
        
        return !fileExists;
    }

    /**
     * Attempts to delete a path on encountering an error.
     * 
     * @param pathToDelete the path to delete
     */

    private static void deletePath( Path pathToDelete )
    {
        // Clean up. This should happen anyway, but is essential for the writer to be "retry friendly" when the 
        // failure to write is recoverable
        LOGGER.debug( "Deleting the following paths that were created before an exception was encountered in the "
                      + "writer: {}.",
                      pathToDelete );

        try
        {
            Files.deleteIfExists( pathToDelete );
        }
        catch ( IOException f )
        {
            LOGGER.error( "Failed to delete a path created before an exception was encountered: {}.",
                          pathToDelete );
        }
    }

    /**
     * Mutates the input, adding a new row.
     *
     * @param <T> the type of values to add
     * @param rows the map of rows to mutate
     * @param sampleMetadata the pool metadata
     * @param values the values to add, one for each column
     * @param formatter an optional formatter
     * @param append is true to add the values to an existing row with the same time window, false otherwise
     * @param durationUnits the duration units for lead times
     * @param additionalComparators one or more additional strings to use in aligning rows
     * @throws NullPointerException if the rows, timeWindow, values or durationUnits are null
     */

    static <T> void addRowToInput( List<RowCompareByLeft> rows,
                                   SampleMetadata sampleMetadata,
                                   List<T> values,
                                   Format formatter,
                                   boolean append,
                                   ChronoUnit durationUnits,
                                   String... additionalComparators )
    {
        Objects.requireNonNull( rows, "Specify one or more rows to mutate." );

        Objects.requireNonNull( sampleMetadata, "Specify the sample metadata." );

        TimeWindowOuter timeWindow = sampleMetadata.getTimeWindow();

        Objects.requireNonNull( timeWindow, "The sample metadata must have a time window." );

        Objects.requireNonNull( values, "Specify one or more values to add." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        StringJoiner row = null;
        int rowIndex = rows.indexOf( RowCompareByLeft.of( timeWindow, null, additionalComparators ) );
        // Set the row to append if it exists and appending is required
        if ( rowIndex > -1 && append )
        {
            row = rows.get( rowIndex ).getRight();
        }
        // Otherwise, start a new row
        else
        {
            row = new StringJoiner( "," );

            // #57932
            String featureName = CommaSeparatedUtilities.getFeatureNameFromMetadata( sampleMetadata );
            String earliestLeadDuration =
                    Long.toString( TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                   durationUnits ) );
            String latestLeadDuration =
                    Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                   durationUnits ) );
            // Add the space/time metadata
            row.add( featureName )
               .add( timeWindow.getEarliestReferenceTime().toString() )
               .add( timeWindow.getLatestReferenceTime().toString() )
               .add( timeWindow.getEarliestValidTime().toString() )
               .add( timeWindow.getLatestValidTime().toString() )
               .add( earliestLeadDuration )
               .add( latestLeadDuration );
            rows.add( RowCompareByLeft.of( timeWindow, row, additionalComparators ) );
        }

        for ( T nextColumn : values )
        {

            String toWrite = MissingValues.STRING;

            // Write the current score component at the current window and threshold
            if ( nextColumn != null )
            {
                toWrite = nextColumn.toString();

                if ( nextColumn instanceof Double && formatter != null
                     && !Double.valueOf( Double.NaN ).equals( nextColumn ) )
                {
                    toWrite = formatter.format( nextColumn );
                }
            }

            // Replace NaN with default missing
            toWrite = toWrite.replace( "NaN", MissingValues.STRING );

            row.add( toWrite );
        }
    }

    /**
     * Returns the first instance of {@link SampleMetadata} discovered in the input or <code>null</code>.
     * 
     * @param <T> the type of statistic
     * @param statistic the list of statistics
     * @return the first sample metadata or null
     * @throws NullPointerException if the input is null
     */

    static <T extends Statistic<?>> SampleMetadata getSampleMetadataFromListOfStatistics( List<T> statistic )
    {
        Objects.requireNonNull( statistic );

        SampleMetadata returnMe = null;

        if ( !statistic.isEmpty() )
        {
            returnMe = statistic.get( 0 )
                                .getMetadata();
        }

        return returnMe;
    }

    /**
     * A helper class that contains a single row whose natural order is based on the {@link TimeWindowOuter} of the row
     * and one or more additional strings, not the contents of the row value.
     *
     * @author james.brown@hydrosolved.com
     */

    static class RowCompareByLeft implements Comparable<RowCompareByLeft>
    {
        /**
         * The row time window.
         */
        private final TimeWindowOuter left;

        /**
         * Optional further comparators.
         */

        private final String[] leftOptions;

        /**
         * The row value.
         */

        private final StringJoiner right;

        /**
         * Returns an instance for the given input.
         *
         * @param timeWindow the time window
         * @param value the row value
         * @param leftOptions the optional additional values for comparison
         * @return an instance
         */

        static RowCompareByLeft of( TimeWindowOuter timeWindow, StringJoiner value, String... leftOptions )
        {
            return new RowCompareByLeft( timeWindow, value, leftOptions );
        }

        /**
         * Returns the left value.
         *
         * @return the left value
         */

        private TimeWindowOuter getLeft()
        {
            return left;
        }

        /**
         * Returns the left options, may be null
         *
         * @return the left options, may be null
         */

        private String[] getLeftOptions()
        {
            return leftOptions;
        }

        /**
         * Returns the right value
         *
         * @return the right value
         */

        private StringJoiner getRight()
        {
            return right;
        }

        @Override
        public int compareTo( RowCompareByLeft compareTo )
        {
            Objects.requireNonNull( compareTo, "Specify a non-null input row for comparison." );

            int returnMe = this.getLeft().compareTo( compareTo.getLeft() );
            if ( returnMe != 0 )
            {
                return returnMe;
            }

            // Check options
            if ( Objects.nonNull( this.getLeftOptions() ) && Objects.isNull( compareTo.getLeftOptions() ) )
            {
                return -1;
            }
            else if ( Objects.nonNull( compareTo.getLeftOptions() ) && Objects.isNull( this.getLeftOptions() ) )
            {
                return 1;
            }
            // Both have non-null options
            else if ( Objects.nonNull( this.getLeftOptions() ) )
            {
                if ( compareTo.getLeftOptions().length < this.getLeftOptions().length )
                {
                    return -1;
                }
                else if ( compareTo.getLeftOptions().length > this.getLeftOptions().length )
                {
                    return 1;
                }
                // Check options
                for ( int i = 0; i < this.getLeftOptions().length; i++ )
                {
                    returnMe = this.getLeftOptions()[i].compareTo( compareTo.getLeftOptions()[i] );
                    if ( returnMe != 0 )
                    {
                        return returnMe;
                    }
                }
            }

            return 0;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof RowCompareByLeft ) )
            {
                return false;
            }

            RowCompareByLeft in = (RowCompareByLeft) o;

            return Objects.equals( in.getLeft(), this.getLeft() )
                   && Arrays.equals( in.getLeftOptions(), this.getLeftOptions() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.getLeft(), this.getLeftOptions() );
        }

        /**
         * Constructor.
         *
         * @param timeWindow the time window
         * @param value the row value
         * @param leftOptions additional comparators for the left
         */

        private RowCompareByLeft( TimeWindowOuter timeWindow, StringJoiner value, String[] leftOptions )
        {
            Objects.requireNonNull( timeWindow, "Specify a non-null time window for the row." );
            this.left = timeWindow;
            this.leftOptions = leftOptions;
            this.right = value;
        }
    }

    /**
     * Constructor.
     *
     * @param projectConfig the project configuration that will drive the writer logic
     * @param durationUnits the time units for lead durations
     * @param outputDirectory the directory into which to write
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if the durationUnits are null
     * @throws IllegalArgumentException if the output directory is not a writable directory
     */

    CommaSeparatedStatisticsWriter( ProjectConfig projectConfig, ChronoUnit durationUnits, Path outputDirectory )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );
        
        // Validate
        File directory = outputDirectory.toFile();
        if ( !directory.isDirectory() || !directory.exists() || !directory.canWrite() )
        {
            throw new IllegalArgumentException( "Cannot create a CSV writer because the path '" + outputDirectory
                                                + "' is not a writable directory." );
        }

        this.projectConfig = projectConfig;
        this.outputDirectory = outputDirectory;
        this.durationUnits = durationUnits;
    }

}
