package wres.io.writing.commaseparated.statistics;

import java.io.BufferedWriter;
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

import wres.config.ProjectConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.TimeWindow;
import wres.util.TimeHelper;

/**
 * Helps write statistics as Comma Separated Values (CSV).
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
abstract class CommaSeparatedStatisticsWriter
{

    /**
     * Delimiter for the header.
     */

    static final String HEADER_DELIMITER = " ";

    /**
     * Earliest possible time window to index the header.
     */

    static final TimeWindow HEADER_INDEX = TimeWindow.of( Instant.MIN,
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
     * @throws IOException if the output cannot be written
     * @throws IllegalArgumentException when any arg is null
     * @throws IllegalArgumentException when rows is empty
     */

    static void writeTabularOutputToFile( List<RowCompareByLeft> rows,
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

        try ( BufferedWriter w = Files.newBufferedWriter( outputPath,
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
    }

    /**
     * Mutates the input, adding a new row.
     *
     * @param <T> the type of values to add
     * @param rows the map of rows to mutate
     * @param timeWindow the time window
     * @param values the values to add, one for each column
     * @param formatter an optional formatter
     * @param append is true to add the values to an existing row with the same time window, false otherwise
     * @param durationUnits the duration units for lead times
     * @param additionalComparators one or more additional strings to use in aligning rows
     * @throws NullPointerException if the rows, timeWindow, values or durationUnits are null
     */

    static <T> void addRowToInput( List<RowCompareByLeft> rows,
                                   TimeWindow timeWindow,
                                   List<T> values,
                                   Format formatter,
                                   boolean append,
                                   ChronoUnit durationUnits,
                                   String... additionalComparators )
    {
        Objects.requireNonNull( rows, "Specify one or more rows to mutate." );

        Objects.requireNonNull( timeWindow, "Specify a time window." );

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
            
            // Until #57932 is addressed, use the reference datetimes by default, but the 
            // valid datetimes when they are bounded and the issued times are unbounded. Fixes #58112
            if ( timeWindow.hasUnboundedReferenceTimes() && !timeWindow.hasUnboundedValidTimes() )
            {
                row.add( timeWindow.getEarliestValidTime().toString() );
                row.add( timeWindow.getLatestValidTime().toString() );
            }
            // Reference times are bounded
            else
            {
                row.add( timeWindow.getEarliestReferenceTime().toString() );
                row.add( timeWindow.getLatestReferenceTime().toString() );
            }

            row.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                    durationUnits ) ) );
            row.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                    durationUnits ) ) );
            rows.add( RowCompareByLeft.of( timeWindow, row, additionalComparators ) );
        }

        for ( T nextColumn : values )
        {

            String toWrite = "NA";

            // Write the current score component at the current window and threshold
            if ( nextColumn != null && !Double.valueOf( Double.NaN ).equals( nextColumn ) )
            {
                if ( formatter != null )
                {
                    toWrite = formatter.format( nextColumn );
                }
                else
                {
                    toWrite = nextColumn.toString();
                }
            }
            row.add( toWrite );
        }
    }

    /**
     * A helper class that contains a single row whose natural order is based on the {@link TimeWindow} of the row
     * and one or more additional strings, not the contents of the row value.
     *
     * @author james.brown@hydrosolved.com
     */

    static class RowCompareByLeft implements Comparable<RowCompareByLeft>
    {
        /**
         * The row time window.
         */
        private final TimeWindow left;

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

        static RowCompareByLeft of( TimeWindow timeWindow, StringJoiner value, String... leftOptions )
        {
            return new RowCompareByLeft( timeWindow, value, leftOptions );
        }

        /**
         * Returns the left value.
         *
         * @return the left value
         */

        private TimeWindow getLeft()
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

        private RowCompareByLeft( TimeWindow timeWindow, StringJoiner value, String[] leftOptions )
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
     */

    CommaSeparatedStatisticsWriter( ProjectConfig projectConfig, ChronoUnit durationUnits, Path outputDirectory )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );

        this.projectConfig = projectConfig;
        this.outputDirectory = outputDirectory;
        this.durationUnits = durationUnits;
    }

}
