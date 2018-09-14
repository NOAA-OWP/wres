package wres.io.writing.commaseparated;

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
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeWindow;
import wres.io.config.ConfigHelper;

/**
 * Helps write files of Comma Separated Values (CSV).
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
abstract class CommaSeparatedWriter
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
                                                          ReferenceTime.VALID_TIME,
                                                          Duration.ofSeconds( Long.MIN_VALUE ) );

    /**
     * Default resolution for writing duration outputs. To change the resolution, change this default.
     */

    static final ChronoUnit DEFAULT_DURATION_UNITS = ChronoUnit.SECONDS;

    /**
     * The project configuration to write.
     */

    final ProjectConfig projectConfig;

    /**
     * Validates the input configuration for writing. Throws an exception if the configuration is invalid.
     * 
     * @param projectConfig the project configuration
     * @throws NullPointerException if the input is null
     * @throws ProjectConfigException if the project is not correctly configured for writing numerical output
     */

    static void validateProjectForWriting( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration when writing outputs." );

        if ( Objects.isNull( projectConfig.getOutputs() )
             || Objects.isNull( projectConfig.getOutputs().getDestination() )
             || projectConfig.getOutputs().getDestination().isEmpty() )
        {
            throw new ProjectConfigException( projectConfig.getOutputs(),
                                              ConfigHelper.OUTPUT_CLAUSE_BOILERPLATE );
        }
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
     * @param additionalComaprators one or more additional strings to use in aligning rows
     */

    static <T> void addRowToInput( List<RowCompareByLeft> rows,
                                   TimeWindow timeWindow,
                                   List<T> values,
                                   Format formatter,
                                   boolean append,
                                   String... additionalComparators )
    {
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
            row.add( timeWindow.getEarliestTime().toString() );
            row.add( timeWindow.getLatestTime().toString() );
            row.add( Long.toString( timeWindow.getEarliestLeadTime().get( DEFAULT_DURATION_UNITS ) ) );
            row.add( Long.toString( timeWindow.getLatestLeadTime().get( DEFAULT_DURATION_UNITS ) ) );
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
     * Returns default header from the {@link SampleMetadata} to which additional information may be appended.
     * 
     * @param sampleMetadata the sample metadata
     * @return default header information
     * @throws NullPointerException if the sampleMetadata is null
     */

    static StringJoiner getDefaultHeaderFromSampleMetadata( SampleMetadata sampleMetadata )
    {
        Objects.requireNonNull( sampleMetadata, "Cannot determine the default CSV header from null metadata." );

        StringJoiner joiner = new StringJoiner( "," );

        String referenceTime = "TIME";
        String timeScale = "";

        // Set the reference time string
        if ( sampleMetadata.hasTimeWindow() )
        {
            referenceTime = sampleMetadata.getTimeWindow().getReferenceTime().name();
            referenceTime = referenceTime.replaceAll( "_", HEADER_DELIMITER );
        }

        // Set the time scale string
        if ( sampleMetadata.hasTimeScale() )
        {
            TimeScale s = sampleMetadata.getTimeScale();

            timeScale = HEADER_DELIMITER
                        + "["
                        + s.getFunction()
                        + HEADER_DELIMITER
                        + "OVER"
                        + HEADER_DELIMITER
                        + "PAST"
                        + HEADER_DELIMITER
                        + s.getPeriod().get( DEFAULT_DURATION_UNITS )
                        + HEADER_DELIMITER
                        + DEFAULT_DURATION_UNITS.name()
                        + "]";
        }

        joiner.add( "EARLIEST" + HEADER_DELIMITER + referenceTime )
              .add( "LATEST" + HEADER_DELIMITER + referenceTime )
              .add( "EARLIEST" + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + "TIME"
                    + HEADER_DELIMITER
                    + "IN"
                    + HEADER_DELIMITER
                    + DEFAULT_DURATION_UNITS.name()
                    + timeScale )
              .add( "LATEST" + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + "TIME"
                    + HEADER_DELIMITER
                    + "IN"
                    + HEADER_DELIMITER
                    + DEFAULT_DURATION_UNITS.name()
                    + timeScale );

        return joiner;
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
     * @param projectConfig the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    CommaSeparatedWriter( ProjectConfig projectConfig )
    {
        // Validate project for writing
        CommaSeparatedWriter.validateProjectForWriting( projectConfig );
        this.projectConfig = projectConfig;
    }

}
