package wres.io.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/*
 * Builder used to programmatically create data providers rather than retrieving
 * the data from an outside source
 */
public class DataBuilder
{
    /**
     * The mapping between column names and their indexes
     */
    private final Map<String, Integer> columnNames;

    /**
     * The listing of each row
     * <br>
     * Each row is represented as an array of objects
     */
    private final List<Object[]> rows;

    /**
     * The index of the row that is currently having values added
     */
    private int currentRow;

    private DataBuilder()
    {
        this.columnNames = new TreeMap<>(  );

        this.rows = new ArrayList<>();
        this.currentRow = -1;
    }

    /**
     * Create a builder with the specified columns
     * @param columnNames The names of each column in the data
     * @return A new builder
     */
    public static DataBuilder with(String... columnNames)
    {
        DataBuilder builder = new DataBuilder();

        if (columnNames.length == 0)
        {
            throw new IllegalArgumentException(
                    "Column names must be added to create a new data set."
            );
        }

        for (int index = 0; index < columnNames.length; ++index)
        {
            builder.columnNames.put(columnNames[index], index);
        }

        return builder;
    }

    /**
     * Progress to the next row
     * @return The modified builder
     */
    public DataBuilder addRow()
    {
        this.currentRow += 1;
        this.rows.add( new Object[columnNames.size()] );
        return this;
    }

    /**
     * Adds a new row with a copy of the given data
     * @param data The data to insert into the new row
     * @return The updated builder
     */
    public DataBuilder addRow(final Object... data)
    {
        if (data.length > this.columnNames.size())
        {
            throw new IndexOutOfBoundsException(
                    "Input array too large; the maximum number of elements is " +
                    this.columnNames.size() +
                    ", but " + data.length + " objects were passed." );
        }

        this.currentRow += 1;
        Object[] newValues = new Object[this.columnNames.size()];

        System.arraycopy( data, 0, newValues, 0, data.length );

        this.rows.add( newValues );
        return this;
    }

    /**
     * Sets the value of the specific column
     * @param columnName The name of the column
     * @param value The value to place in the column
     * @return The updated builder
     */
    public DataBuilder set(final String columnName, final Object value)
    {
        if (!this.columnNames.containsKey( columnName ))
        {
            throw new IllegalArgumentException(
                    "'" + columnName + "' is not a valid column for this dataset."
            );
        }

        if (this.currentRow < 0)
        {
            this.addRow();
        }

        int index = this.columnNames.get( columnName );
        this.rows.get(currentRow)[index] = value;
        return this;
    }

    /**
     * @return A {@link DataProvider} populated with the built data
     */
    public DataProvider build()
    {
        return DataSetProvider.from(this.columnNames, this.rows);
    }
}
