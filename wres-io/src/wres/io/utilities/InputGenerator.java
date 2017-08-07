package wres.io.utilities;

import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.MetricInput;
import wres.io.concurrency.PairRetriever;
import wres.io.config.ConfigHelper;
import wres.io.grouping.LabeledScript;
import wres.util.ProgressMonitor;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator {

    /**
     * Constructor
     * @param projectConfig The project configuration that will guide input creation
     * @param feature The geographic feature configuration that describes the area for the data to retrieve
     */
    public InputGenerator (ProjectConfig projectConfig, Conditions.Feature feature)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
    }

    /**
     * Moves the InputGenerator to the next window
     * @return True if the InputGenerator was able to move to a new window. False if there are no more valid windows
     * @throws SQLException Thrown if the last window could not be calculated based on information in the project
     * configuration and the database
     */
    public boolean next() throws SQLException
    {
        boolean isNext = false;

        if (ConfigHelper.leadIsValid(projectConfig, this.windowNumber + 1, this.getLastWindowNumber()))
        {
            this.windowNumber++;
            isNext = true;
        }

        return isNext;
    }

    /**
     * Generates a new Input wrapper based on the current window
     * @return A newly generated Input wrapper
     */
    public Input getInput()
    {
        Input nextInput = null;

        if (ConfigHelper.leadIsValid(projectConfig, windowNumber, lastWindowNumber))
        {
            PairRetriever retriever = new PairRetriever(this.projectConfig, this.feature, this.windowNumber);
            retriever.setOnRun(ProgressMonitor.onThreadStartHandler());
            retriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            nextInput = new Input(this.projectConfig, this.feature, this.windowNumber, retriever);
        }

        return nextInput;
    }

    /**
     * @return the id for the variable in the database
     * @throws SQLException
     */
    private Integer getVariableID() throws SQLException {
        if (this.variableID == null)
        {
            this.variableID = ConfigHelper.getVariableID(projectConfig.getInputs().getRight());
        }
        return this.variableID;
    }

    /**
     * @return The number of the final window for this project
     * @throws SQLException
     */
    private Integer getLastWindowNumber() throws SQLException
    {
        if (this.lastWindowNumber == null)
        {
            LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(this.getVariableID());

            this.lastWindowNumber = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());
        }
        return this.lastWindowNumber;
    }

    private Integer variableID;
    private Integer lastWindowNumber;
    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;
    private int windowNumber;

    /**
     * Wraps the future results for a MetricInput creation operation and bundles with with useful metadata
     */
    public static class Input implements Comparable<Input>
    {
        /**
         * Constructor
         * @param projectConfig The project configuration that guided the creation of MetricInput creation operation
         * @param feature The configuration for the geographic area to which the MetricInput data will pertain
         * @param windowNumber The sliding window of data that the MetricInput data belongs to
         * @param retriever The function that will create the MetricInput data
         */
        private Input (ProjectConfig projectConfig, Conditions.Feature feature, int windowNumber, PairRetriever retriever)
        {
            this.projectConfig = projectConfig;
            this.feature = feature;
            this.windowNumber = windowNumber;
            this.metricInput = Database.submit(retriever);
        }

        /**
         * @return The feature configuration
         */
        public Conditions.Feature getFeature()
        {
            return this.feature;
        }

        /**
         * @return The iterative number of the window for which the MetricInput data belongs
         */
        public int getWindowNumber()
        {
            return this.windowNumber;
        }

        /**
         * @return A human readable interpretation of the window
         * @throws InvalidPropertiesFormatException Thrown if the configuration for the project does not contain
         * the information needed to interpret different time frames for the data it is attempting to access
         */
        public String getWindowDescription() throws InvalidPropertiesFormatException {
            return ConfigHelper.getLeadQualifier(this.projectConfig, this.windowNumber);
        }

        /**
         * Waits until the MetricInput has been assembled
         * @return MetricInput data shaped per the project configuration's orders
         * @throws ExecutionException
         * @throws InterruptedException
         */
        public MetricInput<?> getMetricInput() throws ExecutionException, InterruptedException
        {
            return this.metricInput.get();
        }

        private final ProjectConfig projectConfig;
        private final Conditions.Feature feature;
        private final int windowNumber;
        private final Future<MetricInput<?>> metricInput;

        @Override
        public boolean equals (final Object obj) {
            return this.hashCode() == obj.hashCode();
        }

        @Override
        public int hashCode () {
            return Objects.hash(this.projectConfig, this.feature, this.windowNumber, this.metricInput);
        }

        @Override
        public int compareTo (final Input other)
        {
            if (this.windowNumber < other.windowNumber)
            {
                return -1;
            }
            else if (this.windowNumber == other.windowNumber)
            {
                return 0;
            }

            return 1;
        }
    }
}
