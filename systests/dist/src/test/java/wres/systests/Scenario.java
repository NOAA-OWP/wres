package wres.systests;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Scenario assocates data regarding a single scenario. Immutable value class.
 */
class Scenario
{
    private final String name;
    private final Path baseDirectory;

    Scenario( String name,
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
