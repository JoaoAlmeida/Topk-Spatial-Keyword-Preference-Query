package s2i.preference;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import s2i.FileTermManager;
import s2i.SpatialInvertedIndex;
import static s2i.SpatialInvertedIndex.textualPartialScore;
import s2i.SpatioItem;
import util.ScoredObject;
import util.cache.MinHeap;
import util.file.ColumnFileException;
import util.nra.Source;
import util.sse.Term;
import util.statistics.StatisticCenter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.Rectangle;

public class PreferenceFileTermManager extends FileTermManager {

    public PreferenceFileTermManager(StatisticCenter statisticCenter, int blockSize,
            String prefixFile, int cacheSize) {
        super(statisticCenter, blockSize, prefixFile, cacheSize);
    }

    public Source<SpatioItem> getSource(Term queryTerm) throws ColumnFileException, IOException {

        final List<SpatioItem> result = new LinkedList<SpatioItem>();
        for (SpatioItem item : this.getList(queryTerm.getTermId())) {
            result.add(new SpatioItem(item.getId(), item.getLatitude(),
                    item.getLongitude(), item.getScore()));

        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
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
   
    public Source<SpatioItem> getSourceNN(final Term queryTerm, final DoublePoint pLocation, double queryLength) throws ColumnFileException, IOException {

        double minDistance = Double.MAX_VALUE;

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        for (SpatioItem featureObj : list) {

            DoublePoint feature = new DoublePoint(new double[]{featureObj.getLatitude(), featureObj.getLongitude()});

            double distance = pLocation.distanceTo(feature);

            if (distance < minDistance) {

                minDistance = distance;

                heap.clear();

                SpatioItem result = new SpatioItem((int) featureObj.getId(), featureObj.getLatitude(), featureObj.getLongitude(),
                        textualPartialScore(queryTerm.getWeight(), queryLength, featureObj.getScore()));

                result.setDistancia(distance);

                heap.add(result);

            } else if (distance == minDistance) {

                SpatioItem result = new SpatioItem((int) featureObj.getId(), featureObj.getLatitude(), featureObj.getLongitude(),
                        textualPartialScore(queryTerm.getWeight(), queryLength, featureObj.getScore()));

                result.setDistancia(distance);

                heap.add(result);
            }

        }
        return new Source<SpatioItem>() {
            public boolean hasNext() {
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

    public Source<SpatioItem> getSourceRange(Term queryTerm, double queryLength, DoublePoint queryLocation, double radius) throws ColumnFileException, IOException {
        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        double distToQuerLocation;
        for (SpatioItem obj : list) {
            distToQuerLocation = queryLocation.distanceTo(new DoublePoint(new double[]{obj.getLatitude(), obj.getLongitude()}));

            if (distToQuerLocation <= radius) {
                double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                        queryLength, obj.getScore());

                SpatioItem item = new SpatioItem(obj.getId(), obj.getLatitude(), obj.getLongitude(), partialTextualScore);

                item.setDistancia(distToQuerLocation);
                heap.add(item);
            }            
        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
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

    public Source<SpatioItem> getSourceInfluence(Term queryTerm, double queryLength, DoublePoint object, double radius) throws ColumnFileException, IOException {
        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        for (SpatioItem feature : list) {

            double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                    queryLength, feature.getScore());

            SpatioItem item = new SpatioItem(feature.getId(), feature.getLatitude(), feature.getLongitude(), partialTextualScore);

            heap.add(item);            
        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
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

    public Source<SpatioItem> getGroupSourceRange(Term queryTerm, double queryLength, Rectangle mbr, double radius) throws ColumnFileException, IOException {

        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        double distToQueryLocation;

        for (SpatioItem feature : list) {

            distToQueryLocation = mbr.minDistance(new DoublePoint(new double[]{feature.getLatitude(), feature.getLongitude()}), 2);

            if (distToQueryLocation <= radius) {

                double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                        queryLength, feature.getScore());

                SpatioItem item = new SpatioItem(feature.getId(), feature.getLatitude(),
                        feature.getLongitude(), partialTextualScore);

                item.setDistancia(distToQueryLocation);
                heap.add(item);
            }
        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
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

    public Source<SpatioItem> getGroupSourceInfluence(Term queryTerm, double queryLength)
            throws ColumnFileException, IOException {

        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        for (SpatioItem feature : list) {

            double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                    queryLength, feature.getScore());

            SpatioItem item = new SpatioItem(feature.getId(), feature.getLatitude(),
                    feature.getLongitude(), partialTextualScore);

            heap.add(item);

        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
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

    public Source<SpatioItem> getGroupSourceNN(Rectangle mbr, List<ScoredObject> objects, Term queryTerm, double queryLength) throws IOException, ColumnFileException {       

        //Inicializa os atributos distance, de cada p em V, com valores altos
        for (ScoredObject p : objects) {
            p.setDistancia(Double.MAX_VALUE);
        }

        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        List<SpatioItem> list = this.getList(queryTerm.getTermId());

        for (SpatioItem featureObj : list) {

            DoublePoint feature = new DoublePoint(new double[]{featureObj.getLatitude(), featureObj.getLongitude()});

            for (ScoredObject pObject : objects) {

                DoublePoint interestObject = new DoublePoint(new double[]{pObject.getLatitude(),
                    pObject.getLongitude()});

                double distance = feature.distanceTo(interestObject);

                if (distance < pObject.getDistancia()) {

                    pObject.setDistancia(distance);

                    heap.clear();

                    SpatioItem result = new SpatioItem((int) featureObj.getId(), featureObj.getLatitude(), featureObj.getLongitude(),
                            textualPartialScore(queryTerm.getWeight(), queryLength, featureObj.getScore()));

                    heap.add(result);

                } else if (distance == pObject.getDistancia()) {

                    SpatioItem result = new SpatioItem((int) featureObj.getId(), featureObj.getLatitude(), featureObj.getLongitude(),
                            textualPartialScore(queryTerm.getWeight(), queryLength, featureObj.getScore()));

                    heap.add(result);
                }
            }
        }
        return new Source<SpatioItem>() {
            public boolean hasNext() {
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
