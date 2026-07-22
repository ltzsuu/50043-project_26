package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*
 * LockManager is responsible for acquiring and releasing locks on behalf of
 * transactions. Implements strict 2PL using page-level locks.
 * 
 * Supports shared (READ_ONLY) & exclusive (READ_WRITE) locks with
 * lock upgrade capability.
 */
public class LockManager {

    // just in case
    private static final long DEADLOCK_TIMEOUT_MS = 500;

    private final Map<PageId, Set<TransactionId>> sharedLocks;
    private final Map<PageId, TransactionId> exclusiveLocks;
    private final Map<TransactionId, Set<PageId>> transactionPages;

    public LockManager() {
        sharedLocks = new ConcurrentHashMap<>();
        exclusiveLocks = new ConcurrentHashMap<>();
        transactionPages = new ConcurrentHashMap<>();
    }

    /*
     * Acquire a lock on behalf of a transaction. If the lock cannot be granted
     * immediately, the thread blocks and waits. If a deadlock timeout expires,
     * throws TransactionAbortedException.
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {

        long deadline = System.currentTimeMillis() + DEADLOCK_TIMEOUT_MS;

        synchronized (this) {
            while (true) {
                if (perm == Permissions.READ_ONLY) {
                    // Exclusive lock covers read permission
                    if (hasExclusiveLock(tid, pid)) {
                        trackLock(tid, pid);
                        return;
                    }
                    // Already hold shared lock
                    if (hasSharedLock(tid, pid)) {
                        return;
                    }
                    // Grant shared if no exclusive holder exists
                    if (!exclusiveLocks.containsKey(pid) || hasExclusiveLock(tid, pid)) {
                        grantShared(tid, pid);
                        return;
                    }
                } else {
                    // READ_WRITE (exclusive)
                    if (hasExclusiveLock(tid, pid)) {
                        return;
                    }
                    // retrieve current shared holders for the page
                    Set<TransactionId> sharedHolders = sharedLocks.getOrDefault(pid, Collections.emptySet());
                    // check if exlucisve lock is already held by another transaction
                    boolean noExclusive = !exclusiveLocks.containsKey(pid);
                    // Can grant exclusive if no exclusive holder and either no shared holders
                    // or only this transaction holds shared locks (upgrade case)
                    boolean canUpgradeOrGrant = noExclusive && (sharedHolders.isEmpty() || (sharedHolders.size() == 1 && sharedHolders.contains(tid)));

                    if (canUpgradeOrGrant) {
                        removeShared(tid, pid);
                        grantExclusive(tid, pid);
                        return;
                    }
                }

                // Lock not available, check for timeout
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    // COUNTDOWN GMTK2026
                    throw new TransactionAbortedException();
                }
                try {
                    wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TransactionAbortedException();
                }
            }
        }
    }

    
    // Release a lock on a page held by a transaction.
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        removeShared(tid, pid);
        if (hasExclusiveLock(tid, pid)) {
            exclusiveLocks.remove(pid);
        }
        Set<PageId> held = transactionPages.get(tid);
        if (held != null) {
            held.remove(pid);
        }
        notifyAll();
    }

    // release all locks held by a tid
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> held = transactionPages.remove(tid); // full set of pages held by this transaction
        if (held != null) {
            // if that page set exists, iterate through and remove the transaction from shared and exclusive locks
            for (PageId pid : held) {
                Set<TransactionId> s = sharedLocks.get(pid);
                if (s != null) {
                    s.remove(tid);
                    if (s.isEmpty()) sharedLocks.remove(pid);
                }
                if (hasExclusiveLock(tid, pid)) {
                    exclusiveLocks.remove(pid);
                }
            }
        }
        notifyAll();
    }

    // check if transaction holds a lock on a page (either shared or exclusive)
    // synchronized is basically wrapping the whole method in synchronized (this) { ... }
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        return hasSharedLock(tid, pid) || hasExclusiveLock(tid, pid);
    }

    // Private helper methods (called within synchronized blocks)
    private boolean hasSharedLock(TransactionId tid, PageId pid) {
        Set<TransactionId> holders = sharedLocks.get(pid);
        return holders != null && holders.contains(tid);
    }


    private boolean hasExclusiveLock(TransactionId tid, PageId pid) {
        return tid.equals(exclusiveLocks.get(pid));
    }

    private void grantShared(TransactionId tid, PageId pid) {
        // check if pid has a set in sharedLocks, if ABSENT, compute (new HashSet) and add it to the map,
        // if present, return the existing set. Then add tid to that set.
        sharedLocks.computeIfAbsent(pid, k -> Collections.synchronizedSet(new HashSet<>())).add(tid);
        trackLock(tid, pid);
    }

    private void grantExclusive(TransactionId tid, PageId pid) {
        exclusiveLocks.put(pid, tid);
        trackLock(tid, pid);
    }

    private void removeShared(TransactionId tid, PageId pid) {
        Set<TransactionId> holders = sharedLocks.get(pid);
        if (holders != null) {
            holders.remove(tid);
            // cleanup stuff
            if (holders.isEmpty()) sharedLocks.remove(pid);
        }
    }

    private void trackLock(TransactionId tid, PageId pid) {
        // track the page in the transaction's held pages set
        // needed for releaseAllLocks to know which pages to release
        transactionPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }
}
