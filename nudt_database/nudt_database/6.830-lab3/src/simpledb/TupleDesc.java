package simpledb;

import java.awt.font.NumericShaper;
import java.util.*;

import javax.lang.model.type.TypeVisitor;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc
{
	
	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	
	public int					numFields;
	public ArrayList<Type>		typeArrayList	= new ArrayList<Type>();
	public ArrayList<String>	nameArrayList	= new ArrayList<String>();
	
	public static TupleDesc combine(TupleDesc td1, TupleDesc td2)
	{
		// some code goes here
		TupleDesc returnTd = new TupleDesc();
		
		returnTd.numFields = td1.numFields + td2.numFields;
		
		returnTd.typeArrayList.addAll(td1.typeArrayList);
		returnTd.nameArrayList.addAll(td1.nameArrayList);
		
		returnTd.typeArrayList.addAll(td2.typeArrayList);
		returnTd.nameArrayList.addAll(td2.nameArrayList);
		
		return returnTd;
	}
	
	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr)
	{
		// some code goes here
		numFields = typeAr.length;
		for (int i = 0; i < numFields; i++)
		{
			//System.out.println(typeAr[i]);
			typeArrayList.add(typeAr[i]);
			nameArrayList.add(fieldAr[i]);
		}
		
	}
	
	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr)
	{
		// some code goes here
		numFields = typeAr.length;
		for (int i = 0; i < numFields; i++)
		{
			typeArrayList.add(typeAr[i]);
			nameArrayList.add("");
		}
	}
	
	/**
	 * @return the defaul constructor set numFields.
	 */
	public TupleDesc()
	{
		numFields = 0;
	}
	
	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields()
	{
		// some code goes here
		return numFields;
	}
	
	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException
	{
		// some code goes here
		try
		{
			return nameArrayList.get(i);
		}
		catch (Exception e)
		{
			throw new IndexOutOfBoundsException(
					"The index is out of the TypleDesc");
			// TODO: handle exception
		}
		
	}
	
	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int nameToId(String name) throws NoSuchElementException
	{
		// some code goes here
		
		int index = nameArrayList.indexOf(name);
		if (index < 0)
			throw new NoSuchElementException(String.format(
					"No filedName is %s", name));
		else
			return index;
	}
	
	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getType(int i) throws NoSuchElementException
	{
		// some code goes here
		try
		{
			return typeArrayList.get(i);
		}
		catch (Exception e)
		{
			throw new IndexOutOfBoundsException(
					"The index is out of the TypleDesc");
			// TODO: handle exception
		}
		
	}
	
	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize()
	{
		// some code goes here
		int Size = 0;
		for (int i = 0; i < numFields; i++)
			Size += typeArrayList.get(i).getLen();
		return Size;
	}
	
	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o)
	{
		
		// some code goes here
		if (o instanceof TupleDesc)
		{
			TupleDesc td = (TupleDesc) o;
			if (getSize() != td.getSize())
				return false;
			for (int i = 0; i < numFields; i++)
				if (typeArrayList.get(i) != td.typeArrayList.get(i))
					return false;
			return true;
		}
		else
			return false;
	}
	
	public int hashCode()
	{
		int value = 0;
		for (int i = 0; i < numFields; i++)
			value += typeArrayList.get(i).hashCode() * 37 + numFields;
		return value;
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		
		// throw new UnsupportedOperationException("unimplemented");
	}
	
	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString()
	{
		// some code goes here
		String tmp = "";
		
		for (int i = 0; i < numFields; i++)
			tmp += String.format("%-15s%-15s", typeArrayList.get(i).toString(),
					nameArrayList.get(i).toString());
		
		return tmp;
	}
}
