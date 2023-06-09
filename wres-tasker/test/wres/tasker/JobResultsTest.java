package wres.tasker;


import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobResultsTest
{
    private FileSystem fileSystem;
    private Path testingDirectory;

    private final String TEST = "testing";

    @BeforeEach
    public void setUp() throws IOException
    {
        fileSystem = Jimfs.newFileSystem( Configuration.unix() );
        testingDirectory = fileSystem.getPath( TEST );
        Files.createDirectory( testingDirectory );
    }

    @AfterEach
    public void tearDown() throws IOException
    {
        fileSystem.close();
    }

    @Test
    void testGetFilesInDirectory() throws IOException
    {
        Files.createFile( testingDirectory.resolve( "testFile1.txt" ) );
        Files.createFile( testingDirectory.resolve( "testFile2.txt" ) );
        Files.createFile( testingDirectory.resolve( "testFile3.txt" ) );
        List<Path> paths = JobResults.getFilesInDirectory( testingDirectory );
        assertEquals( 3, paths.size() );
    }

    @Test
    void testSortFilesByLastModified() throws IOException
    {
        List<Path> files = new ArrayList<>();
        Path file1 = fileSystem.getPath( "testFile1.txt" );
        Path file2 = fileSystem.getPath( "testFile2.txt" );
        Path file3 = fileSystem.getPath( "testFile3.txt" );

        Files.createFile( file1 );
        Files.createFile( file2 );
        Files.createFile( file3 );

        Instant modifiedTime1 = Instant.parse( "2023-03-07T12:00:00Z" );
        Instant modifiedTime2 = Instant.parse( "2023-03-08T12:00:00Z" );
        Instant modifiedTime3 = Instant.parse( "2023-03-09T12:00:00Z" );

        Files.setLastModifiedTime( file1, FileTime.from( modifiedTime1 ) );
        Files.setLastModifiedTime( file2, FileTime.from( modifiedTime2 ) );
        Files.setLastModifiedTime( file3, FileTime.from( modifiedTime3 ) );

        files.add( file1 );
        files.add( file2 );
        files.add( file3 );

        Collections.shuffle( files );

        JobResults.sortFilesByLastModified( files );

        assertEquals( file1, files.get( 0 ) );
        assertEquals( file2, files.get( 1 ) );
        assertEquals( file3, files.get( 2 ) );
    }

    @Test
    void testGetFileLastModified() throws IOException
    {
        Path file = fileSystem.getPath( "testFile.txt" );
        Files.createFile( file );

        Instant expectedLastModifiedTime = Instant.parse( "2023-03-08T12:00:00Z" );
        Files.setLastModifiedTime( file, FileTime.from( expectedLastModifiedTime ) );

        assertEquals( expectedLastModifiedTime.toEpochMilli(), JobResults.getFileLastModified( file ) );
    }

    @Test
    void testDeleteFile() throws IOException
    {
        Path file = fileSystem.getPath( "fileToDelete.txt" );
        Files.createFile( file );

        assertTrue( Files.exists( file ) );

        JobResults.deleteFile( file );

        assertFalse( Files.exists( file ) );
    }
}