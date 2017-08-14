package wres.io.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.MetricInput;
import wres.io.concurrency.InputRetriever;
import wres.io.config.ConfigHelper;
import wres.io.grouping.LabeledScript;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InputGenerator.class);
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

    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator ()
    {
        return new MetricInputIterator(this.projectConfig, this.feature);
    }

    private static final class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
    {
        private int windowNumber;
        private Integer lastWindowNumber;
        private Integer variableID;

        private final Conditions.Feature feature;
        private final ProjectConfig projectConfig;

        public MetricInputIterator(ProjectConfig projectConfig, Conditions.Feature feature)
        {
            this.projectConfig = projectConfig;
            this.feature = feature;
        }

        private Integer getLastWindowNumber() throws SQLException
        {
            if (this.lastWindowNumber == null)
            {
                LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(this.getVariableID());
                this.lastWindowNumber = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());

                // If the last window number could not be determined from the database, set it to a number that should
                // always yield false on validity checks
                if (this.lastWindowNumber == null)
                {
                    this.lastWindowNumber = -1;
                }
            }
            return this.lastWindowNumber;
        }

        /**
         * @return the id for the variable in the database
         * @throws SQLException
         */
        private Integer getVariableID() throws SQLException
        {
            if (this.variableID == null)
            {
                this.variableID = ConfigHelper.getVariableID(projectConfig.getInputs().getRight());
            }
            return this.variableID;
        }

        @Override
        public boolean hasNext () {
            boolean isNext = false;

            try
            {
                isNext = ConfigHelper.leadIsValid(projectConfig, this.windowNumber + 1, this.getLastWindowNumber());
            }
            catch (SQLException error)
            {
                LOGGER.error("The last window for this project could not be calculated.");
                LOGGER.error(Strings.getStackTrace(error));
            }

            return isNext;
        }

        @Override
        public Future<MetricInput<?>> next () {
            Future<MetricInput<?>> nextInput = null;

            if (ConfigHelper.leadIsValid(this.projectConfig, this.windowNumber + 1, this.lastWindowNumber))
            {
                this.windowNumber++;
                InputRetriever retriever = new InputRetriever(this.projectConfig, this.feature, this.windowNumber);
                retriever.setOnRun(ProgressMonitor.onThreadStartHandler());
                retriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                nextInput = Database.submit(retriever);
            }

            return nextInput;
        }
    }
}
