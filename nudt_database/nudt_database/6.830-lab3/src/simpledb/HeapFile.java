package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile
{
	
	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	
	private File		_file;
	private TupleDesc	_tupleDesc;
	private int			_oldPages;
	private int _newPages;
	
	public HeapFile(File f, TupleDesc td)
	{
		_file = f;
		_tupleDesc = td;
		_oldPages = (int) (_file.length() / BufferPool.PAGE_SIZE);
		_newPages = 0;
		// some code goes here
	}
	
	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile()
	{
		// some code goes here
		return _file;
	}
	
	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId()
	{
		// some code goes here
		return _file.getAbsolutePath().hashCode();
		// throw new UnsupportedOperationException("implement this");
	}
	
	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc()
	{
		// some code goes here
		return _tupleDesc;
		// throw new UnsupportedOperationException("implement this");
	}
	
	// see DbFile.java for javadocs
	public Page readPage(PageId pid)
	{
		// some code goes here
		byte[] buf = new byte[BufferPool.PAGE_SIZE];
		FileInputStream fis = null;
		HeapPage p = null;
		try
		{
			fis = new FileInputStream(_file);
			fis.skip(pid.pageno() * BufferPool.PAGE_SIZE);
			fis.read(buf);
			fis.close();
			p = new HeapPage((HeapPageId) pid, buf);
			return p;
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
			// TODO: handle exception
		}
		return p;
	}
	
	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException
	{
		// some code goes here
		//_newPages++;
		RandomAccessFile accessFile = new RandomAccessFile(_file, "rws");
		accessFile.seek(page.getId().pageno() * BufferPool.PAGE_SIZE);
		accessFile.write(page.getPageData());
		accessFile.close();
		// not necessary for lab1
	}
	
	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages()
	{
		// some code goes here
		// return _numberPages;
		return _oldPages + _newPages;
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException
	{
		// some code goes here
		for(int i = 0;i<numPages();i++)
		{
			PageId pid = new HeapPageId(getId(), i);
			HeapPage p = (HeapPage)GetPage(tid, pid, Permissions.READ_WRITE);
			if(p.getNumEmptySlots() > 0)
			{
				//System.out.println(t.toString());
				//System.out.println(numPages());
				System.out.println("The insert Page is "+ p.toString());
				p.markDirty(true, tid);
				p.addTuple(t);
				ArrayList<Page> arrayListPage = new ArrayList<Page>();
				arrayListPage.add(p);
				return arrayListPage;
			}
		}
		
		PageId pid = new HeapPageId(getId(), numPages());
		_newPages ++;
		HeapPage p = (HeapPage)GetPage(tid, pid, Permissions.READ_WRITE);
		p.markDirty(true, tid);
		p.addTuple(t);
		ArrayList<Page> arrayListPage = new ArrayList<Page>();
		arrayListPage.add(p);
		return arrayListPage;
		// not necessary for lab1
	}
	
	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException
	{
		// some code goes here
		
		PageId pid = t.getRecordId().getPageId();
		if(getId() != pid.getTableId())
			throw new DbException("The tuple is not in this file");
		
		HeapPage hp = (HeapPage)GetPage(tid, pid, Permissions.READ_WRITE);
		hp.deleteTuple(t);
		return hp;
	}
	
	public Page GetPage(TransactionId tid,PageId pid,Permissions perm) throws TransactionAbortedException, DbException
	{
		return Database.getBufferPool().getPage(tid, pid, perm);
	}
	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid)
	{
		// some code goes here
		return (DbFileIterator) new HeapFileIterator(this, tid);
	}
	
}
