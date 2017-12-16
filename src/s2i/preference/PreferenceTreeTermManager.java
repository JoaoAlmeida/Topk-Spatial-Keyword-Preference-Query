package s2i.preference;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import s2i.SpatialInvertedIndex;
import static s2i.SpatialInvertedIndex.textualPartialScore;
import s2i.SpatioItem;
import s2i.SpatioTreeHeapEntry;
import s2i.TreeTermManager;
import util.ScoredObject;
import util.cache.MaxHeap;
import util.cache.MinHeap;
import util.nra.Source;
import util.sse.Term;
import util.statistics.StatisticCenter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.Rectangle;
import xxl.util.MaxDoubleRTree;
import xxl.util.MaxDoubleRectangle;
import xxl.core.indexStructures.ORTree.IndexEntry;

public class PreferenceTreeTermManager extends TreeTermManager {

    public PreferenceTreeTermManager(StatisticCenter statisticCenter, String outputPath,
            int treeDimensions, int treeCacheSize, int treeBlockSize,
            int treeNodeMinCapacity, int treeNodeMaxCapacity, int treeManagerCacheSize) {
        super(statisticCenter, outputPath, treeDimensions, treeCacheSize, treeBlockSize,
                treeNodeMinCapacity, treeNodeMaxCapacity, treeManagerCacheSize);
    }

    public Source<SpatioItem> getSourceNN(final Term queryTerm, final DoublePoint queryLocation, final double queryLength) throws IOException, ClassNotFoundException {

        MaxDoubleRTree tree = get(queryTerm.getTermId());
        double minDistance = Double.MAX_VALUE;

        //Stores the itens in ascending order of distance, the score is the distance
        final MinHeap<SpatioTreeHeapEntry> heap = new MinHeap<SpatioTreeHeapEntry>();

        final MinHeap<SpatioItem> result = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        heap.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!heap.isEmpty()) {

            SpatioTreeHeapEntry entry = heap.poll();

            if (entry.isData() && entry.getScore() < minDistance) {

                result.clear();
                minDistance = entry.getScore();

                double partialScore = textualPartialScore(queryTerm.getWeight(), queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                //coloca o escore no lugar correto novamente
                SpatioItem resultItem = new SpatioItem(entry.getId(),
                        entry.getMBR().getCorner(false).getValue(0),
                        entry.getMBR().getCorner(false).getValue(1),
                        //the distance that was used as score is changed to term impact
                        partialScore); //only the term impact

                resultItem.setDistancia(minDistance); //set the distance                        

                result.add(resultItem);

            } else if (entry.isData() && entry.getScore() == minDistance) {

                double partialScore = textualPartialScore(queryTerm.getWeight(), queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                //coloca o escore no lugar correto novamente
                SpatioItem resultItem = new SpatioItem(entry.getId(),
                        entry.getMBR().getCorner(false).getValue(0),
                        entry.getMBR().getCorner(false).getValue(1),
                        //the distance that was used as score is changed to term impact
                        partialScore); //only the term impact

                resultItem.setDistancia(entry.getScore()); //set the distance                        

                result.add(resultItem);

            } else if (entry.isData() && entry.getScore() > minDistance) {
                break;
            } else if (!entry.isData()) {
                for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {
                    entry = new SpatioTreeHeapEntry(it.next());

                    if (entry.getMBR().minDistance(queryLocation, 2) <= minDistance) {
                        //the score is the distance       
                        entry.setScore(entry.getMBR().minDistance(queryLocation, 2));

                        heap.add(entry);
                    }
                }
            }
        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
                return !result.isEmpty();
            }

            public SpatioItem next() {
                if (!result.isEmpty()) {
                    return result.poll();
                } else {
                    return null;
                }
            }
        };
    }

    public Source<SpatioItem> getSourceRange(final Term queryTerm, final double queryLength, final DoublePoint queryLocation, final double radius) throws IOException, ClassNotFoundException {
        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        MaxDoubleRTree tree = get(queryTerm.getTermId());
        MaxHeap<SpatioTreeHeapEntry> treeEntries = new MaxHeap<>();
        treeEntries.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!treeEntries.isEmpty()) {
            SpatioTreeHeapEntry entry = treeEntries.poll();

            if (entry.isData()) {

                double distance = queryLocation.distanceTo(new DoublePoint(new double[]{entry.getMBR().getCorner(false).getValue(0),
                    entry.getMBR().getCorner(false).getValue(1)}));

                if (distance <= radius) {
                    SpatioItem resultItem = new SpatioItem(entry.getId(),
                            entry.getMBR().getCorner(false).getValue(0),
                            entry.getMBR().getCorner(false).getValue(1),
                            entry.getScore());

                    resultItem.setDistancia(distance); //set the distance   
                    heap.add(resultItem);
                }
            } else {
                for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {

                    entry = new SpatioTreeHeapEntry(it.next());

                    if (entry.getMBR().minDistance(queryLocation, 2) <= radius) {

                        double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                                queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                        entry.setScore(partialTextualScore);
                        treeEntries.add(entry);
                    }
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

    //o mesmo método no SIA é utilizado no SIA+, uma vez que todos objetos indexados atendem ao critério de vizinhança influence
    public Source<SpatioItem> getSourceInfluence(final Term queryTerm, final double queryLength, final DoublePoint queryLocation, final double radius) throws IOException, ClassNotFoundException {
        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        MaxDoubleRTree tree = get(queryTerm.getTermId());
        MaxHeap<SpatioTreeHeapEntry> treeEntries = new MaxHeap<>();
        treeEntries.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!treeEntries.isEmpty()) {
            SpatioTreeHeapEntry entry = treeEntries.poll();

            if (entry.isData()) {

                SpatioItem resultItem = new SpatioItem(entry.getId(),
                        entry.getMBR().getCorner(false).getValue(0),
                        entry.getMBR().getCorner(false).getValue(1),
                        entry.getScore());

                heap.add(resultItem);

            } else {
                for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {

                    entry = new SpatioTreeHeapEntry(it.next());

                    double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                            queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                    entry.setScore(partialTextualScore);

                    treeEntries.add(entry);
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

    public Source<SpatioItem> getGroupSourceNN(Rectangle mbr, List<ScoredObject> list, final Term queryTerm, final double queryLength) throws IOException, ClassNotFoundException {

        MaxDoubleRTree tree = get(queryTerm.getTermId());

        double maxDistance = Double.MAX_VALUE;

        //Inicializa os atributos distance, de cada p em V, com valores altos
        for (ScoredObject p : list) {
            p.setDistancia(Double.MAX_VALUE);
            p.nnMode();
        }

        //Stores the itens in ascending order of distance, the score is the distance
        final MinHeap<SpatioTreeHeapEntry> heap = new MinHeap<>();

        final MaxHeap<Double> boundHeap = new MaxHeap<>();

        final TreeSet<SpatioItem> result = new TreeSet<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });;

        heap.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!heap.isEmpty()) {

            SpatioTreeHeapEntry entry = heap.poll();

            if (entry.isData()) {

                DoublePoint feature = new DoublePoint(new double[]{entry.getMBR().getCorner(false).getValue(0),
                    entry.getMBR().getCorner(false).getValue(1)});

                for (ScoredObject pObject : list) {

                    DoublePoint interestObject = new DoublePoint(new double[]{pObject.getLatitude(),
                        pObject.getLongitude()});

                    double distance = feature.distanceTo(interestObject);

                    if (distance < pObject.getDistancia()) {

                        pObject.setDistancia(distance);
                        boundHeap.add(distance);

                        double partialScore = textualPartialScore(queryTerm.getWeight(), queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                        //coloca o escore no lugar correto novamente
                        SpatioItem resultItem = new SpatioItem(entry.getId(),
                                entry.getMBR().getCorner(false).getValue(0),
                                entry.getMBR().getCorner(false).getValue(1),
                                //the distance that was used as score is changed to term impact
                                partialScore); //only the term impact                                   

                        pObject.getNn().clear();
                        pObject.getNn().add(resultItem);

                    } else if (distance == pObject.getDistancia()) {

                        double partialScore = textualPartialScore(queryTerm.getWeight(), queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                        //coloca o escore no lugar correto novamente
                        SpatioItem resultItem = new SpatioItem(entry.getId(),
                                entry.getMBR().getCorner(false).getValue(0),
                                entry.getMBR().getCorner(false).getValue(1),
                                //the distance that was used as score is changed to term impact
                                partialScore); //only the term impact                                   

                        pObject.getNn().add(resultItem);
                    }
                }
                //a heap retorna a maior distancia obtida entre um p e f
                if (boundHeap.peek() != null) {
                    if (boundHeap.peek() < maxDistance) {
                        maxDistance = boundHeap.peek();
                    }
                }

                boundHeap.clear();

            } else if (entry.getScore() > maxDistance) {
                break;
            } else {
                for (Iterator it = ((IndexEntry) entry.getItem()).get().entries(); it.hasNext();) {
                    entry = new SpatioTreeHeapEntry(it.next());
                    //the score is the distance                    
                    double mbrDist = entry.getMBR().distance(mbr, 2);

                    entry.setScore(mbrDist);

                    if (mbrDist <= maxDistance) {
                        heap.add(entry);
                    } else if (entry.isData()) {
                        heap.add(entry);
                    }
                }
            }
        }

        for (ScoredObject item : list) {
            for (int a = 0; a < item.getNn().size(); a++) {
                result.add(item.getNn().get(a));
            }
        }

        return new Source<SpatioItem>() {
            public boolean hasNext() {
                return !result.isEmpty();
            }

            public SpatioItem next() {
                return result.pollFirst();
            }
        };
    }

    public Source<SpatioItem> getGroupSourceRange(final Term queryTerm, final double queryLength, final Rectangle mbr, final double radius) throws IOException, ClassNotFoundException {
        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        MaxDoubleRTree tree = get(queryTerm.getTermId());
        MaxHeap<SpatioTreeHeapEntry> treeEntries = new MaxHeap<>();
        treeEntries.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!treeEntries.isEmpty()) {
            SpatioTreeHeapEntry entry = treeEntries.poll();

            if (entry.isData()) {

                double distance = mbr.minDistance(new DoublePoint(new double[]{entry.getMBR().getCorner(false).getValue(0),
                    entry.getMBR().getCorner(false).getValue(1)}), 2);

                if (distance <= radius) {
                    SpatioItem resultItem = new SpatioItem(entry.getId(),
                            entry.getMBR().getCorner(false).getValue(0),
                            entry.getMBR().getCorner(false).getValue(1),
                            entry.getScore());

                    resultItem.setDistancia(distance);
                    heap.add(resultItem);
                }
            } else {
                IndexEntry indexEntry = (IndexEntry) entry.getItem();
                for (Iterator it = indexEntry.get().entries(); it.hasNext();) {

                    entry = new SpatioTreeHeapEntry(it.next());

                    if (entry.getMBR().distance(mbr, 2) <= radius) {

                        double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                                queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                        entry.setScore(partialTextualScore);
                        treeEntries.add(entry);
                    }
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

    public Source<SpatioItem> getGroupSourceInfluence(final Term queryTerm, final double queryLength, final Rectangle mbr) throws IOException, ClassNotFoundException {
        //Ordered by id
        final MinHeap<SpatioItem> heap = new MinHeap<>(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return ((SpatioItem) o1).getId() - ((SpatioItem) o2).getId();
            }
        });

        MaxDoubleRTree tree = get(queryTerm.getTermId());
        MaxHeap<SpatioTreeHeapEntry> treeEntries = new MaxHeap<>();
        treeEntries.add(new SpatioTreeHeapEntry(tree.rootEntry()));

        while (!treeEntries.isEmpty()) {
            SpatioTreeHeapEntry entry = treeEntries.poll();

            if (entry.isData()) {

                SpatioItem resultItem = new SpatioItem(entry.getId(),
                        entry.getMBR().getCorner(false).getValue(0),
                        entry.getMBR().getCorner(false).getValue(1),
                        entry.getScore());

                heap.add(resultItem);

            } else {
                IndexEntry indexEntry = (IndexEntry) entry.getItem();
                for (Iterator it = indexEntry.get().entries(); it.hasNext();) {

                    entry = new SpatioTreeHeapEntry(it.next());

                    double partialTextualScore = SpatialInvertedIndex.textualPartialScore(queryTerm.getWeight(),
                            queryLength, ((MaxDoubleRectangle) entry.getMBR()).getScore());

                    entry.setScore(partialTextualScore);
                    treeEntries.add(entry);
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
