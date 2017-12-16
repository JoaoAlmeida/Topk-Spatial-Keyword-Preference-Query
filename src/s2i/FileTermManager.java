/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import util.cache.MaxHeap;
import util.file.BufferedListStorage;
import util.file.ColumnFileException;
import util.nra.Source;
import util.sse.Term;
import util.statistics.StatisticCenter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.Rectangle;

/**
 *
 * @author joao
 */
//transformei em publico
public class FileTermManager extends BufferedListStorage<SpatioItem> {

    public FileTermManager(StatisticCenter statisticCenter, int blockSize,
            String prefixFile, int cacheSize) {
        super(statisticCenter, "fileTermManager", prefixFile, cacheSize,
                SpatioItem.SIZE, SpatioItem.FACTORY);
    }

    /**
     * Returns the objects inside the mbr. The score of the objects is the term
     * impact!
     */
    Source<SpatioItem> getSourceMBR(Term queryTerm, Rectangle mbr) throws ColumnFileException, IOException {

        final List<SpatioItem> result = new LinkedList<SpatioItem>();
        for (SpatioItem item : this.getList(queryTerm.getTermId())) {
            if (mbr.contains(new DoublePoint(new double[]{item.getLatitude(), item.getLongitude()}))) {
                result.add(new SpatioItem(item.getId(), item.getLatitude(),
                        item.getLongitude(), item.getScore()));
            }
        }

        return new Source<SpatioItem>() {
            public boolean hasNext(){
                return !result.isEmpty();
            }
            public SpatioItem next() {
                if (!result.isEmpty()) {
                    return result.remove(0);
                } else {
                    return null;
                }
            }
        };
    }

    
    Source<SpatioItem> getSource(final Term queryTerm, final int queryKeywords, final double queryLength,
            final DoublePoint queryLocation, final double maxDist, final double alpha) throws ColumnFileException, IOException {

        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        final MaxHeap<SpatioItem> heap = new MaxHeap<SpatioItem>();

        double termScore = 0;
        double spatioScore = 0;       
        for (SpatioItem obj : list) {

            termScore = SpatialInvertedIndex.textualPartialScore(
                    queryTerm.getWeight(), queryLength, obj.getScore());
            spatioScore = SpatialInvertedIndex.spatioPartialScore(
                    queryKeywords, queryLocation.distanceTo(new DoublePoint(new double[]{obj.getLatitude(), obj.getLongitude()})),
                    maxDist, alpha);
            heap.add(new SpatioItem(obj.getId(), obj.getLatitude(),
                    obj.getLongitude(), termScore + spatioScore, spatioScore));
            /*       distance = queryLocation.distanceTo(new DoublePoint(new double[]{obj.getLatitude(), obj.getLongitude()}));
             heap.add(new SpatioItem(obj.getId(),obj.getLatitude(),
             obj.getLongitude(), SpatialInvertedIndex.score(queryTerm,
             queryLength, distance, new Term(obj.getId(), obj.getScore()), maxDist, alpha)));
             */
        }

        return new Source<SpatioItem>() {
            public boolean hasNext(){
                return !heap.isEmpty();
            }
            public SpatioItem next() {
                if (!heap.isEmpty()) {
                    return heap.poll();
                } else {
                    return null;
                }
            }
        };
    }
}
