package simpledb;

import java.util.ArrayList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple
{
	
	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc
	 *            instance with at least one field.
	 */
	private TupleDesc		TdInstance;
	public ArrayList<Field>	fieldArrayList	= new ArrayList<Field>();
	private RecordId		tpRecordId;
	
	public Tuple(TupleDesc td)
	{
		// some code goes here
		TdInstance = td;
		for (int i = 0; i < td.numFields; i++)
			fieldArrayList.add(null);
		
	}
	
	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return TdInstance;
	}
	
	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 *         be null.
	 */
	
	public RecordId getRecordId()
	{
		// some code goes here
		return tpRecordId;
	}
	
	/**
	 * Set the RecordId information for this tuple.
	 * 
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid)
	{
		// some code goes here
		tpRecordId = rid;
	}
	
	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f)
	{
		try
		{
			fieldArrayList.set(i, f);
		}
		catch (Exception e)
		{
			// TODO: handle exception
			throw new IndexOutOfBoundsException("The index is illegal");
			
		}
		
		// some code goes here
	}
	
	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i)
	{
		// some code goes here
		try
		{
			return fieldArrayList.get(i);
		}
		catch (Exception e)
		{
			// TODO: handle exception
			throw new IndexOutOfBoundsException("The index is illegal");
		}
		
	}
	
	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 *
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
	 *
	 * where \t is any whitespace, except newline, and \n is a newline
	 */
	public String toString()
	{
		// some code goes here
		// throw new UnsupportedOperationException("Implement this");
		String contentString = "";
		for (int i = 0; i < TdInstance.numFields - 1; i++)
			contentString += fieldArrayList.get(i).toString() + " ";
		contentString += fieldArrayList.get(TdInstance.numFields - 1)
				.toString() + "\n";
		return contentString;
	}
}
