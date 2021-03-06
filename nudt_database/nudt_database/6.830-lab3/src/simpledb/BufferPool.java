package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool
{
	/** Bytes per page, including header. */
	public static final int		PAGE_SIZE		= 4096;
	
	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int		DEFAULT_PAGES	= 50;
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	private int					_numPages;
	private Map<PageId, Page>	_pageIdToPage;
	//private AnotherPageLock plock;
	private PageLock plock;
	public BufferPool(int numPages)
	{
		_numPages = numPages;
		_pageIdToPage = new HashMap<PageId, Page>();
		
		//plock = new AnotherPageLock();
		plock = new PageLock();
		// some code goes here
	}
	
	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 * @throws IOException 
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException
	{
		//System.out.println(pid.toString());
		plock.lock(tid, pid, perm);
		Page page = _pageIdToPage.get(pid);
		if (page != null)
			return page;
		if (_pageIdToPage.size() >= _numPages)
			evictPage();
			//throw new DbException("There is too much pages");
		
		page = GetNewPage(pid);
		_pageIdToPage.put(pid, page);
		return page;
		// some code goes here
	}
	
	public Page GetNewPage(PageId pid)
	{
		return Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
	}
	
	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid)
	{
		// some code goes here
		plock.unlock(tid, pid);
		// not necessary for lab1|lab2
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException
	{
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid,true);
	}
	
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p)
	{
		// some code goes here
		// not necessary for lab1|lab2
		return plock.holdsLock(tid, p);
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException
	{
		try
		{
			if(commit)
				Commit(tid);
			else
				Abort(tid);
		}
		finally
		{
			plock.unlockAll(tid);
		}
		// some code goes here
		// not necessary for lab1|lab2
	}
	
	public void Commit(TransactionId tid) throws IOException
	{
		Set<PageId> set= plock.GetPageIdAccordTran(tid);
		
		for(PageId pid:set)
		{
			HeapPage p=(HeapPage) _pageIdToPage.get(pid);
			if(p != null && p.isDirty()!=null)
			{
				flushPage(pid);
				p.markDirty(false, tid);
			}
		}
	}
	
	public void Abort(TransactionId tid)
	{
		Set<PageId> set= plock.GetPageIdAccordTran(tid);
		for(PageId pid:set)
		{
			HeapPage p=(HeapPage) _pageIdToPage.get(pid);
			if(p != null && p.isDirty()!=null)
			{
				_pageIdToPage.remove(pid);
				p = (HeapPage)GetNewPage(pid);
				_pageIdToPage.put(pid, p);
			}
		}
	}
	
	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to(Lock acquisition
	 * is not needed for lab2). May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException
	{
		// some code goes here
		HeapFile f = (HeapFile)Database.getCatalog().getDbFile(tableId);
		f.addTuple(tid, t);
		// not necessary for lab1
	}
	
	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from. May block if the lock cannot
	 * be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit. Does not need to update cached versions of any pages
	 * that have been dirtied, as it is not possible that a new page was created
	 * during the deletion (note difference from addTuple).
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException
	{
		// some code goes here
		int tableId = t.getRecordId().getPageId().getTableId();
		HeapFile f = (HeapFile)Database.getCatalog().getDbFile(tableId);
		f.deleteTuple(tid, t);
		// not necessary for lab1
	}
	
	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException
	{
		// some code goes here
		// not necessary for lab1
		for(PageId p : _pageIdToPage.keySet())
		{
			flushPage(p);
		}
		
	}
	
	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid)
	{
		// some code goes here
		// only necessary for lab5
	}
	
	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException
	{
		// some code goes here
		HeapPage hp = (HeapPage)_pageIdToPage.get(pid);
		HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(pid.getTableId());
		//if(hp.isDirty() !=null)
		//{
			hf.writePage(hp);
			hp.markDirty(false, new TransactionId());
		//}
		
		// not necessary for lab1
	}
	
	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException
	{
		// some code goes here
		// not necessary for lab1|lab2|lab3
	}
	
	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 * @throws IOException 
	 */
	private synchronized void evictPage() throws DbException
	{
		// some code goes here
		// not necessary for lab1
		Iterator<PageId> it = _pageIdToPage.keySet().iterator();
		try
		{
			while(it.hasNext())
			{
				PageId pid = it.next();
				HeapPage hp = (HeapPage)_pageIdToPage.get(pid);
				if(hp.isDirty() == null)
				{
					flushPage(pid);
					System.out.println("the evit page is " + hp.toString());
					_pageIdToPage.remove(pid);
					return ;
				}
			}
			throw new DbException("There isn's appropriate page to evict");
		}
		catch (Exception e)
		{
			// TODO: handle exception
			DbException de = new DbException("");
			de.initCause(e);
			throw de;
			
		}
		
			
		
			//System.out.println(pid);
			
			
		
		
		
	}
	
}
