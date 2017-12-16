/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import util.FileUtils;
import util.Util;
import util.cache.CacheLRU;
import util.cache.MaxHeap;
import util.nra.Source;
import util.sse.Term;
import util.statistics.StatisticCenter;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.util.MaxDoubleRTree;
import xxl.util.MaxDoubleRectangle;
import xxl.core.indexStructures.ORTree.IndexEntry;
import xxl.core.spatial.points.DoublePoint;

/**
 *
 * @author joao
 */
public class TreeTermManager {

    //Parameters to construct an MaxIntegerTreee
    private final StatisticCenter statisticCenter;
    private final String outputPath;
    private final int treeDimensions;
    private final int treeCacheSize;
    private final int treeBlockSize;
    private final int treeNodeMinCapacity;
    private final int treeNodeMaxCapacity;
    private TreeCache<Integer, MaxDoubleRTree> cache;
    private final int treeManagerCacheSize;
    
    public TreeTermManager(StatisticCenter statisticCenter, String outputPath,
            int treeDimensions, int treeCacheSize, int treeBlockSize,
            int treeNodeMinCapacity, int treeNodeMaxCapacity, int treeManagerCacheSize) {
        this.statisticCenter = statisticCenter;
        this.outputPath = outputPath;
        this.treeDimensions = treeDimensions;
        this.treeCacheSize = treeCacheSize;
        this.treeBlockSize = treeBlockSize;
        this.treeNodeMinCapacity = treeNodeMinCapacity;
        this.treeNodeMaxCapacity = treeNodeMaxCapacity;
        this.treeManagerCacheSize = treeManagerCacheSize;
    }

    public void open() {
        cache = new TreeCache<>(treeManagerCacheSize);
    }

    public MaxDoubleRTree get(int termId) throws IOException, ClassNotFoundException {
        MaxDoubleRTree cachedTree = cache.get(termId);
        if (cachedTree == null) {
            //Check if the tree exists (by the filename) and load it
            cachedTree = createTre(termId);
            cachedTree.open();
            cache.put(termId, cachedTree);
        }
        return cachedTree;
    }

    public void flush() throws IOException {
        //make the data in the cache persistent
        int count = 0;
        long time = System.currentTimeMillis();
        System.out.print("[Flushing...");

        for (Map.Entry<Integer, MaxDoubleRTree> entry : cache.entrySet()) {
            count++;
            entry.getValue().flush();
        }

        System.out.print("" + count + " trees in" + Util.time(System.currentTimeMillis() - time) + "]");
    }

    Source<SpatioItem> getSourceMBR(final Term queryTerm, final Rectangle mbr) throws IOException, ClassNotFoundException {

        MaxDoubleRTree tree = this.get(queryTerm.getTermId());

        final LinkedList<SpatioTreeHeapEntry> heap = new LinkedList<SpatioTreeHeapEntry>();

        heap.add(new SpatioTreeHeapEntry(tree.rootEntry()));


        return new Source<SpatioItem>() {
            double termScore = 0;

            public boolean hasNext() {
                return !heap.isEmpty();
            }

            public SpatioItem next() {
                while (!heap.isEmpty()) {
                    SpatioTreeHeapEntry entry = heap.poll();

                    if (entry.isData()) {
                        return new SpatioItem(entry.getId(),
                                entry.getMBR().getCorner(false).getValue(0),
                                entry.getMBR().getCorner(false).getValue(1),
                                ((MaxDoubleRectangle) entry.getMBR()).getScore(), //only the term impact
                                entry.getLowerBoundSpatialScore());
                    } else {
                        for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {
                            entry = new SpatioTreeHeapEntry(it.next());

                            if (mbr.overlaps(entry.getMBR())) {
                                heap.add(entry);
                            }
                        }
                    }
                }
                return null;
            }
        };
    }

    Source<SpatioItem> getSource(final Term queryTerm, final int queryKeywords, final double queryLength,
            final DoublePoint queryLocation, final double maxDist, final double alpha) throws IOException, ClassNotFoundException {

        MaxDoubleRTree tree = this.get(queryTerm.getTermId());

        final MaxHeap<SpatioTreeHeapEntry> heap = new MaxHeap<SpatioTreeHeapEntry>();

        heap.add(new SpatioTreeHeapEntry(tree.rootEntry()));


        return new Source<SpatioItem>() {
            public boolean hasNext() {
                return !heap.isEmpty();
            }

            public SpatioItem next() {
                double termScore = 0;
                double spatioScore = 0;
                while (!heap.isEmpty()) {
                    SpatioTreeHeapEntry entry = heap.poll();

                    //filtrar aqui
                    if (entry.isData()) {
                        return new SpatioItem(entry.getId(),
                                entry.getMBR().getCorner(false).getValue(0),
                                entry.getMBR().getCorner(false).getValue(1),
                                entry.getScore(), entry.getLowerBoundSpatialScore());
                        //filtrar aqui
                    } else {
                        for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {
                            entry = new SpatioTreeHeapEntry(it.next());

                            termScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                                    queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());
                            spatioScore = SpatialInvertedIndex.spatioPartialScore(
                                    queryKeywords, entry.getMBR().minDistance(queryLocation, 2),
                                    maxDist, alpha);

                            entry.setScore(termScore + spatioScore);
                            entry.setLowerBoundSpatialScore(spatioScore);                      

                            heap.add(entry);
                        }
                    }
                }
                return null;
            }
        };
    }

    public void close() throws IOException {
        for (Map.Entry<Integer, MaxDoubleRTree> entry : cache.entrySet()) {
            entry.getValue().close();
        }
        this.cache.clear();
        this.cache = null;
    }

    private MaxDoubleRTree createTre(int termId) {
        String prefix = getDirectory(termId) + "/tree_" + termId;
        
        return new MaxDoubleRTree(statisticCenter, "trees_", prefix,
                treeDimensions, treeCacheSize, treeBlockSize,
                treeNodeMinCapacity, treeNodeMaxCapacity);
    }

    private String getDirectory(int id) {
        String directory = this.outputPath + "/trees";
        for (int i = 2; i > 0; i--) {
            directory += "/" + (id / (int) Math.pow(1000, i));
        }
        FileUtils.createDirectories(directory);
        return directory;
    }

    public long getSizeInBytes() {
        return FileUtils.getSizeInBytes(this.outputPath + "/trees");
    }

    private class TreeCache<K extends Integer, V extends MaxDoubleRTree> extends CacheLRU<K, V> {

        private int cacheSize;

        public TreeCache(int cacheSize) {
            super(cacheSize);
            this.cacheSize = cacheSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() > this.cacheSize) {
                try {
                    eldest.getValue().close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
