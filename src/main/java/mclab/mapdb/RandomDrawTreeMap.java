package mclab.mapdb;


import java.io.Closeable;
import java.util.AbstractMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class RandomDrawTreeMap<K,V>
        extends AbstractMap<K,V>
        implements ConcurrentMap<K,V>,Closeable {
    protected static final Logger LOG = Logger.getLogger(HTreeMap.class.getName());

    /**
     * Lu's comment
     * the largest nodes under a same slot
     */
    public int BUCKET_OVERFLOW = 4;
    public int BUCKET_LENGTH = 28;

    protected int DIRECTORY_NODE_SIZE = 0;
    protected int NUM_BITS_PER_COMPARISON = 0;
    protected int BITS_COMPARISON_MASK = 0;
    protected int BITMAP_SIZE = 0;
    protected int MAX_TREE_LEVEL = 0;
    protected int TOTAL_HASH_LENGTH = 0;

    public int SEG = 0;

    /**
     * is this a Map or Set?  if false, entries do not have values, only keys are allowed
     */
    protected final boolean hasValues;

    /**
     * Salt added to hash before rehashing, so it is harder to trigger hash collision attack.
     */
    protected final int hashSalt;

    /**
     * serializer for key
     */
    protected final Serializer<K> keySerializer;
    /**
     * serializer for value
     */
    protected final Serializer<V> valueSerializer;

    /**
     * engines is the Map for every different engine, which keep different sub-index
     * Integer---->sub-index-ID, Engine---->index structure, where keep the data as (recid,binary form)
     */
    public final ConcurrentHashMap<Integer, Engine> engines = new ConcurrentHashMap<>();

    /**
     * Snapshot for different sub-index
     */
    protected final ConcurrentHashMap<Integer, Engine> snapshots = new ConcurrentHashMap<>();

    /**
     * Indicate whether the engine is closed
     */
    protected final boolean closeEngine;

    /**
     * Defalut value creater(used when there is no such key in the Map)
     */
    protected final Fun.Function1<V, K> valueCreator;

    /**
     * RAM storage threshold
     */
    protected final long ramThreshold;

    /**
     * controlling the simulation of default MapDB, indicate whether this Map is locked
     */
    protected ReentrantReadWriteLock initStorageLock = new ReentrantReadWriteLock();

    /**
     * It indicate whether certain sub-index is initalized
     */
    protected boolean defaultMapDBInitialized[]= null;









}
