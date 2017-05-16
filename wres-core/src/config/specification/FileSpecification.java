/**
 * 
 */
package config.specification;

/**
 * @author Christopher Tubbs
 *
 */
public class FileSpecification
{

    public FileSpecification(String fileType, String path)
    {
        this.fileType = fileType;
        this.path = path;
    }

    public String getFileType()
    {
        return this.fileType;
    }
    
    public String getPath()
    {
        return this.path;
    }
    
    @Override
    public String toString() {
        String description = "\tFile: ";
        description += getPath();
        description += ", Type: ";
        description += getFileType();
        description += System.lineSeparator();
        
        return description;
    }
    
    private final String fileType;
    private final String path;
}
