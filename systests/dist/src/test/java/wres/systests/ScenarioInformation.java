package wres.systests;

import java.nio.file.Path;

/**
 * This stores information about a system test scenario. Immutable value class.
 */
class ScenarioInformation
{
    /**
     * The name of the scenario.  This corresponds to the name of the subdirectory
     * within the {@link #baseDirectory}.
     */
    private final String name;
    
    /**
     * The base directory storing the system test scenarios.
     */
    private final Path baseDirectory;

    protected ScenarioInformation( String name,
              Path baseDirectory )
    {
        this.name = name;
        this.baseDirectory = baseDirectory;
    }

    Path getBaseDirectory()
    {
        return this.baseDirectory;
    }

    Path getScenarioDirectory()
    {
        return this.baseDirectory.resolve( this.name );
    }

    String getName()
    {
        return this.name;
    }
}
