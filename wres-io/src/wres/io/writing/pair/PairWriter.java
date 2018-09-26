package wres.io.writing.pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.io.concurrency.WRESRunnableException;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.util.CalculationException;

/**
 * Returns a string to be written to a pairs file.
 */
public class PairWriter implements Supplier<Pair<Path,String>>
{

    /*
     * TODO: currently writing of paired outputs is not coordinated by the ProductProcessor like all other outputs.
     * The ProductProcessor coordinates the ChronoUnit resolution for writing outputs. Given the above, the near-term
     * workaround is to specify a separate default here. The near-term solution is to remove this default by including
     * the preferred units inline to the project declaration: see #55441.
     * 
     * Long term, we don't want to write paired outputs separately. These should be included inline to our other 
     * outputs as diagnostic information, or products that use pairs (e.g. time-series progress plots) should be
     * products in their own right. See #54942. Long-term, this class will disappear completely, subject to agreement.
     */
    
    static final ChronoUnit DEFAULT_DURATION_UNITS = ChronoUnit.SECONDS;
    
    private static final Logger LOGGER = LoggerFactory.getLogger( PairWriter.class );
    private static final String NEWLINE = System.lineSeparator();
    private static final String DELIMITER = ",";
    private static final String PAIR_FILENAME = "/pairs.csv";
    private static final String BASELINE_FILENAME = "/baseline_pairs.csv";
    
    private final DestinationConfig destinationConfig;
    private final Instant date;
    private final Feature feature;
    private final int leadIteration;
    private final EnsemblePair pair;
    private final boolean isBaseline;
    private final int poolingStep;
    private final ProjectDetails projectDetails;
    private final Duration lead;
    private final DecimalFormat formatter;

    /**
     * Build a {@link PairWriter} with a mutable builder. 
     */
    
    public static class Builder
    {
        private DestinationConfig destinationConfig;
        private Instant date;
        private Feature feature;
        private int leadIteration;
        private EnsemblePair pair;
        private boolean isBaseline;
        private int poolingStep;
        private ProjectDetails projectDetails;
        private Duration lead;
        private DecimalFormat formatter;
        
        /**
         * Set the destination configuration
         * @param destinationConfig the destination configuration
         * @return the builder
         */

        public Builder setDestinationConfig(DestinationConfig destinationConfig)
        {
            this.destinationConfig = destinationConfig;
            return this;
        }

        /**
         * Sets the date.
         * @param date the date
         * @return the builder
         */
        
        public Builder setDate(Instant date)
        {
            this.date = date;
            return this;
        }

        /**
         * Sets the feature.
         * @param feature the feature
         * @return the builder
         */
        public Builder setFeature(Feature feature)
        {
            this.feature = feature;            
            return this;
        }

        /**
         * Sets the lead time iteration.
         * @param leadIteration the lead iteration
         * @return the builder
         */
        
        public Builder setLeadIteration(Integer leadIteration)
        {
            this.leadIteration = leadIteration;
            return this;
        }

        /**
         * Sets the pair
         * @param pair the pair
         * @return the builder
         */
        
        public Builder setPair(EnsemblePair pair)
        {
            this.pair = pair;
            return this;
        }

        /**
         * Sets the baseline status as being present (<code>true</code>) or absent (<code>false</code>).
         * @param isBaseline is true if the baseline is present, otherwise false
         * @return the builder
         */
        
        public Builder setIsBaseline(boolean isBaseline)
        {
            this.isBaseline = isBaseline;
            return this;
        }

        /**
         * Sets the pooling step.
         * @param poolingStep the pooling step.
         * @return the builder
         */
        
        public Builder setPoolingStep(Integer poolingStep)
        {
            this.poolingStep = poolingStep;
            return this;
        }

        /**
         * Sets the project details.
         * @param projectDetails the project details
         * @return the builder
         */
        
        public Builder setProjectDetails(ProjectDetails projectDetails)
        {
            this.projectDetails = projectDetails;
            return this;
        }

        /**
         * Sets the lead duration.
         * @param lead the lead duration.
         * @return the builder
         */
        
        public Builder setLead(Duration lead)
        {
            this.lead = lead;
            return this;
        }
        
        /**
         * Sets the decimal formatter.
         * 
         * @param formatter the formatter
         * @return the builder
         */
        
        public Builder setFormatter(DecimalFormat formatter)
        {
            this.formatter = formatter;
            return this;
        }

        /**
         * Builds the pair writer.
         * @return the pair writer
         */
        
        public PairWriter build()
        {
            return new PairWriter( this );
        }
    }

    /**
     * Builds a pair writer.
     * @param builder the builder
     * @throws NullPointerException if one or more required inputs is null
     */
    
    private PairWriter( Builder builder )
    {
        // Set then validate        
        this.destinationConfig = builder.destinationConfig;
        this.date = builder.date;
        this.feature = builder.feature;
        this.leadIteration = builder.leadIteration;
        this.pair = builder.pair;
        this.isBaseline = builder.isBaseline;
        this.poolingStep = builder.poolingStep;
        this.projectDetails = builder.projectDetails;
        this.lead = builder.lead;
        
        // Tries to set the default formatter when missing
        if (builder.formatter == null)
        {
            String configuredFormat = this.getDestinationConfig().getDecimalFormat();

            if ( configuredFormat != null && !configuredFormat.isEmpty() )
            {
                this.formatter = new DecimalFormat();
                this.formatter.applyPattern( configuredFormat );
            }
            else 
            {
                this.formatter = null;
            }
        }
        else
        {
            this.formatter = builder.formatter;
        }
        
        // Report on all fails collectively
        int errorCount = 0;
        StringJoiner errorJoiner = new StringJoiner( NEWLINE );

        if (this.destinationConfig == null)
        {
            errorCount += 1;
            errorJoiner.add( "There was no destination passed to write to.");
        }

        if (this.date == null)
        {
            errorCount += 1;
            errorJoiner.add("No date was added to record.");
        }

        if (this.feature == null)
        {
            errorCount += 1;
            errorJoiner.add("No feature was added to record.");
        }

        if (this.leadIteration == Integer.MIN_VALUE)
        {
            errorCount += 1;
            errorJoiner.add("The iteration was not added to record.");
        }

        if (this.pair == null)
        {
            errorCount += 1;
            errorJoiner.add("No pair was added to record.");
        }

        if (this.projectDetails == null)
        {
            errorCount += 1;
            errorJoiner.add("No details about the project were passed.");
        }

        if (errorCount > 0)
        {
            throw new IllegalArgumentException( "A PairWriter could not be "
                                                + "created: " +
                                                errorJoiner.toString() );
        }
    }

    @Override
    public Pair<Path,String> get()
    {
        File directoryFromDestinationConfig =
                ConfigHelper.getDirectoryFromDestinationConfig( this.getDestinationConfig() );

        String actualFileDestination = directoryFromDestinationConfig.toString();

        try
        {
            actualFileDestination =
                    directoryFromDestinationConfig.getCanonicalPath();
        }
        catch ( IOException ioe )
        {
            // Not critical to get the full path, keep the original and keep going.
            LOGGER.warn( "Could not get canonical path for {}",
                         directoryFromDestinationConfig );
        }

        if (this.isBaseline)
        {
            actualFileDestination += BASELINE_FILENAME;
        }
        else
        {
            actualFileDestination += PAIR_FILENAME;
        }

        Path destination = Paths.get( actualFileDestination );

        StringJoiner line = new StringJoiner( DELIMITER );

        line.add( ConfigHelper.getFeatureDescription( this.getFeature() ) );

        // Avoid changing date format to iso format because benchmarks
        line.add( this.date.toString()
                           .replace( "T", " " )
                           .replace( "Z", "" ) );

        // But above could be as simple as this (and be more precise):
        //line.add( this.date.toString() );

        line.add( String.valueOf( this.getLeadDuration().toHours() ) );
                
        try
        {
            line.add( this.getWindow() );
        }
        catch ( CalculationException e )
        {
            throw new WRESRunnableException( "Pairs could not be gotten for " +
                                             ConfigHelper.getFeatureDescription( this.feature ),
                                             e );
        }

        line.add(this.getLeftValue());
        line.add(this.getRightValues());

        String toWrite = line.toString();

        return Pair.of( destination, toWrite );
    }

    private DestinationConfig getDestinationConfig()
    {
        return this.destinationConfig;
    }

    private Feature getFeature()
    {
        return this.feature;
    }
    
    private Duration getLeadDuration()
    {
        return this.lead;
    }

    private String getWindow() throws CalculationException
    {

        int window = this.getWindowNum();

        // If basis time pooling is used, you get intermediary pools. This means
        // that you don't just get entries for window 0, 1, 2, 3, 4, etc, you
        // get window 0 pooling step 1, window 0 pooling step 2, window 1
        // pooling step 1, etc. To find the overall window (i.e. "this is the
        // fifth calculation"), you need to break down the calculation to
        // compensate for the number of intermediate windows
        if ( this.projectDetails.getPairingMode() == ProjectDetails.PairingMode.ROLLING )
        {
            window *= (this.projectDetails.getIssuePoolCount( this.feature ));
            window += this.poolingStep;
        }

        window++;

        return String.valueOf(window);
    }

    private int getWindowNum()
    {
        return this.leadIteration;
    }

    private String getLeftValue()
    {

        double leftValue = pair.getLeft();
        String left;

        if ( Double.compare( leftValue, Double.NaN ) == 0 )
        {
            left = "NaN";
        }
        else if ( this.getFormatter() != null )
        {
            left = this.getFormatter().format( leftValue );
        }
        else
        {
            left = String.valueOf( leftValue ) ;
        }

        return left;
    }

    private String getRightValues()
    {
        double[] rightValues = pair.getRight();
        StringJoiner arrayJoiner = new StringJoiner( DELIMITER );

        Arrays.sort( rightValues );

        for ( Double rightValue : rightValues )
        {
            if ( rightValue.isNaN() )
            {
                arrayJoiner.add( "NaN" );
            }
            else if ( this.getFormatter() != null )
            {
                arrayJoiner.add( this.getFormatter().format( rightValue ) );
            }
            else
            {
                arrayJoiner.add( String.valueOf( rightValue ) );
            }
        }

        return arrayJoiner.toString();
    }

    private DecimalFormat getFormatter()
    {
        return this.formatter;
    }


}
