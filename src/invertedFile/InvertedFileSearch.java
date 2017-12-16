/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package invertedFile;

import util.DefaultSearch;
import util.LoadRTree;
import util.ScoredObject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import s2i.SpatialInvertedIndex;
import s2i.SpatioTreeHeapEntry;
import util.IFTuple;
import util.SpatioTextualObject;
import util.cache.MinHeap;
import util.config.Settings;
import util.experiment.ExperimentException;
import util.file.BufferedListStorage;
import util.file.ColumnFileException;
import util.file.EntryStorage;
import util.file.IntegerEntry;
import util.sse.Term;
import util.sse.Vector;
import util.sse.Vocabulary;
import util.statistics.DefaultStatisticCenter;
import util.statistics.StatisticCenter;
import xxl.core.cursors.Cursor;
import xxl.util.StarRTree;

/**
 *
 * @author Robos
 */
public class InvertedFileSearch extends DefaultSearch {

    private final BufferedListStorage<IFTuple> cache;
    private final String docLengthFile;
    private HashMap<Integer, Integer> documentLength;
    //maps term id (from termVocabulary) to the number of documents that has the term (document frequency of the term)
    private EntryStorage<IntegerEntry> termInfo;
    private final String termInfoFile;
    private final int neighborhood;
    private final StarRTree objectsOfInterest;
    private final double radius;

    public InvertedFileSearch(StatisticCenter statisticCenter, boolean debug, Vocabulary termVocabulary,
            int numKeywords, int numResults, int numQueries, double alpha,
            double spaceMaxValue, String queryType, String queryKeywords, int numMostFrequentTerms,
            int numWarmUpQueries, String ifFile, int cacheSize, String docLengthFile,
            String termInfoFile, int neighborhood, StarRTree objectsOfInterest, double radius) {

        super(statisticCenter, debug, termVocabulary, numKeywords, numResults,
                numQueries, alpha, spaceMaxValue, queryType, queryKeywords, numMostFrequentTerms, numWarmUpQueries);

        this.cache = new BufferedListStorage<>(statisticCenter, "ifSearch",
                ifFile, cacheSize, IFTuple.SIZE, IFTuple.FACTORY);

        this.docLengthFile = docLengthFile;
        this.termInfoFile = termInfoFile;

        this.neighborhood = neighborhood;
        this.objectsOfInterest = objectsOfInterest;
        this.radius = radius;
    }

    @Override
    public void open() throws ExperimentException {

        try {
            termVocabulary.open();
            termInfo = new EntryStorage<>(statisticCenter, "termInfo",
                    termInfoFile, IntegerEntry.SIZE, IntegerEntry.FACTORY);
            termInfo.open();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }

        super.open();

        try {
            this.objectsOfInterest.open();
            this.cache.open();

            FileInputStream fileInputStream = new FileInputStream(docLengthFile);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            documentLength = (HashMap<Integer, Integer>) objectInputStream.readObject();
            objectInputStream.close();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    @Override
    protected void collectStatistics(int count) {
        long termInfoBlocksRead = statisticCenter.getCount("termInfo_blocksRead").getValue();
        statisticCenter.getTally("avgTermInfo_blocksRead").update(termInfoBlocksRead / (double) count);

        long mapBlocksRead = statisticCenter.getCount("ifSearch_map_blocksRead").getValue();
        long ifBlocksRead = statisticCenter.getCount("ifSearch_blocksRead").getValue();
        statisticCenter.getTally("avgIfSearch_map_blocksRead").update(mapBlocksRead / (double) count);
        statisticCenter.getTally("avgIfSearch_blocksRead").update(ifBlocksRead / (double) count);
        statisticCenter.getTally("avgPageFault").update((mapBlocksRead + ifBlocksRead) / (double) count);

        long ifSearchListCacheFault = statisticCenter.getCount("ifSearchListCacheFault").getValue();
        statisticCenter.getTally("avgIfSearchListCacheFault").update(ifSearchListCacheFault / (double) count);

        long nodesAccessed = statisticCenter.getCount("nodesAccessed").getValue();
        statisticCenter.getTally("avgNodesAccessed").update(nodesAccessed / (double) count);

        long entriesAcessed = statisticCenter.getCount("ifSearchEntriesAccessed").getValue();
        statisticCenter.getTally("avgIfSearchEntriesAccessed").update(entriesAcessed / (double) count);
    }

    @Override
    protected int getDocumentFrequency(int termId) {
        try {
            IntegerEntry entry = termInfo.getEntry(termId);
            return entry == null ? -1 : entry.getValue();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected int getTotalNumObjects() {
        return documentLength.size();
    }

    @Override
    protected Iterator<SpatioTextualObject> execute(double queryLatitude, double queryLongitude, double maxDist, String queryKeywords, int k, double alpha) throws ExperimentException {
        try {

            long time = System.currentTimeMillis();

            System.out.println("Searching... " + queryKeywords + "");

            TreeSet<SpatioTextualObject> topK = new TreeSet<>();

            Vector queryVector = new Vector();
            Vector.vectorize(queryVector, queryKeywords, termVocabulary);
            double wq = Vector.computeQueryWeight(queryVector, getTotalNumObjects(), termInfo);

            Cursor leaves = objectsOfInterest.query(1);
            while (leaves.hasNext()) {
                SpatioTreeHeapEntry leafEntry = new SpatioTreeHeapEntry(leaves.next());
                Cursor interestPointer = objectsOfInterest.query(leafEntry.getMBR());

                while (interestPointer.hasNext()) {
                    SpatioTreeHeapEntry point = new SpatioTreeHeapEntry(interestPointer.next());
                    ScoredObject obj = new ScoredObject(point.getId(),
                            point.getMBR().getCorner(false).getValue(0),
                            point.getMBR().getCorner(false).getValue(1));

                    if (neighborhood == 0) {
                        IFNearestSearch(queryVector, obj, wq);
                    } else if (neighborhood == 1) {
                        IFRangeSearch(queryVector, radius, obj, wq);
                    } else if (neighborhood == 2) {
                        IFInflueceSearch(queryVector, radius, obj, wq);
                    } else {
                        throw new ExperimentException("The neighborhood " + neighborhood + "' is not defined yet!!!");
                    }

                    if (topK.size() < k) {
                        topK.add(obj);
                    } else if (obj.getScore() > topK.first().getScore()
                            || // keep the best objects, if they have the same scores, keeps the objects with smaller ids
                            (obj.getScore() == topK.first().getScore() && obj.getId() > topK.first().getId())) {
                        topK.pollFirst();
                        topK.add(obj);
                    }
                }
                interestPointer.close();
            }
            leaves.close();
            statisticCenter.getTally("avgQueryProcessingTime").update(System.currentTimeMillis() - time);

            return topK.descendingIterator();
        } catch (Exception ex) {
            throw new ExperimentException(ex);
        }
    }

    public void influenceScore(ScoredObject object, FeatureEntry entry) {

        double distToObject = DefaultSearch.euclideanDistance(object.getLatitude(), object.getLongitude(),
                entry.feature.getLatitude(), entry.feature.getLongitude());

        entry.textualScore = entry.textualScore * Math.pow(2, -distToObject / radius);
        
        if ((entry.textualScore > object.getScore())) { //transformar em updateScore depois, se remover aquela verificação da distancia                                                
                object.setScore(entry.textualScore);
            }
    }

    public void IFNearestSearch(Vector query, final ScoredObject p, double wq) throws IOException, ColumnFileException {
        
        //Inicializa a distância com um valor bem grande        
        double minDistance = Double.MAX_VALUE;
        
        Iterator terms = query.iterator();

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        while (terms.hasNext()) {
            Term term = (Term) terms.next();

            List<IFTuple> list = cache.getList(term.getTermId());
            //sort list by id
            Collections.sort(list, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    return (int) (((IFTuple) o1).getID() - ((IFTuple) o2).getID());
                }
            });

            if (list != null && list.size() > 0) {
                Iterator<IFTuple> it = list.iterator();
                heap.add(new FeatureEntry(it.next(), it, term, wq));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);

        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);
            while (other != null && entry.feature.getID() == other.feature.getID()) {
                //update the score of the feature
                entry.incScore(other.textualScore);
                other = nextEntry(heap);
            }
            minDistance = updateScoreNN(p, entry, minDistance);

            entry = other;
        }
        if (entry != null) {
            minDistance = updateScoreNN(p, entry, minDistance);
        }
    }

    public void IFRangeSearch(Vector query, double radius, ScoredObject p, double wq) throws IOException, ColumnFileException {

        Iterator terms = query.iterator();

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        while (terms.hasNext()) {
            Term term = (Term) terms.next();

            List<IFTuple> list = cache.getList(term.getTermId());
            //sort list by id
            Collections.sort(list, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    return (int) (((IFTuple) o1).getID() - ((IFTuple) o2).getID());
                }
            });

            if (list != null && list.size() > 0) {
                Iterator<IFTuple> it = list.iterator();
                heap.add(new FeatureEntry(it.next(), it, term, wq));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);

        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);
            while (other != null && entry.feature.getID() == other.feature.getID()) {
                //update the score of the feature
                entry.incScore(other.textualScore);
                other = nextEntry(heap);
            }
            updateScore(p, entry, radius);

            entry = other;
        }
        if (entry != null) {
            updateScore(p, entry, radius);
        }
    }

    public void IFInflueceSearch(Vector query, double range, ScoredObject object, double wq) throws IOException, ColumnFileException {

        Iterator terms = query.iterator();

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        while (terms.hasNext()) {
            Term term = (Term) terms.next();

            List<IFTuple> list = cache.getList(term.getTermId());
            //sort list by id
            Collections.sort(list, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    return (int) (((IFTuple) o1).getID() - ((IFTuple) o2).getID());
                }
            });

            if (list != null && list.size() > 0) {
                Iterator<IFTuple> it = list.iterator();
                heap.add(new FeatureEntry(it.next(), it, term, wq)); //cria o feature e calcula o escore textual
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);

        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            while (other != null && entry.feature.getID() == other.feature.getID()) {
                //update the score of the feature
                entry.incScore(other.textualScore);
                other = nextEntry(heap);
            }

            influenceScore(object, entry);
                        
            entry = other;
        }
        if (entry != null) {
            influenceScore(object, entry);            
        }
    }

    private static void updateScore(ScoredObject p, FeatureEntry entry, double radius) {
        if ((DefaultSearch.euclideanDistance(p.getLatitude(), p.getLongitude(),
                entry.feature.getLatitude(), entry.feature.getLongitude()) < radius) && (entry.textualScore > p.getScore())) {
            p.setScore(entry.textualScore);
        }
    }

    private double updateScoreNN(ScoredObject p, FeatureEntry entry, double minDistance) {

        double distance = DefaultSearch.euclideanDistance(p.getLatitude(), p.getLongitude(),
                entry.feature.getLatitude(), entry.feature.getLongitude());

        if (distance < minDistance) {           
            
            minDistance = distance;
            p.setScore(entry.textualScore);

        } else if (distance == minDistance && entry.textualScore > p.getScore()) {
            p.setScore(entry.textualScore);
        }   
        
        return minDistance;
    }

    private FeatureEntry nextEntry(MinHeap<FeatureEntry> heap) {
        FeatureEntry entry = heap.poll();
        if (entry != null && entry.source.hasNext()) {
            heap.add(new FeatureEntry(entry.source.next(), entry.source, entry.term, entry.queryLength));
        }
        return entry;
    }

    @Override
    public void close() throws ExperimentException {
        try {
            termVocabulary.close();
            termInfo.close();
            objectsOfInterest.close();
        } catch (Exception e) {
            throw new ExperimentException();


        }
    }

    private class FeatureEntry implements Comparable {

        final Iterator<IFTuple> source;
        final IFTuple feature;
        final Term term;
        final double queryLength;
        double textualScore;

        public FeatureEntry(IFTuple feature, Iterator<IFTuple> source, Term term, double queryLength) {
            this.feature = feature;
            this.term = term;
            this.queryLength = queryLength;
            this.source = source;

            textualScore = SpatialInvertedIndex.textualPartialScore(term.getWeight(),
                    queryLength, feature.getTermImpact());
        }

        public void incScore(double partialScore) {
            this.textualScore += partialScore;
        }

        @Override
        public int compareTo(Object o) {
            FeatureEntry other = (FeatureEntry) o;
            return (int) (feature.getID() - other.feature.getID());
        }
    }

    public static void main(String[] args) throws Exception {
        Properties properties = Settings.loadProperties("framework.properties");
        String prefix = properties.getProperty("experiment.folder");

        Vocabulary vocabulary = new Vocabulary(prefix + "/" + properties.getProperty("if.vocabulary"));
        DefaultStatisticCenter statistics = new DefaultStatisticCenter();
        boolean debug = true;

        StarRTree rTree = new StarRTree(statistics, "",
                properties.getProperty("if.folder") + "/rtree",
                Integer.parseInt(properties.getProperty("srtree.dimensions")),
                Integer.parseInt(properties.getProperty("srtree.cacheSize")),
                Integer.parseInt(properties.getProperty("disk.blockSize")),
                Integer.parseInt(properties.getProperty("srtree.tree.minNodeCapacity")),
                Integer.parseInt(properties.getProperty("srtree.tree.maxNodeCapacity")));

        LoadRTree.load(rTree, properties.getProperty("dataset.objectsFile"));

        InvertedFileSearch searching = new InvertedFileSearch(statistics,
                debug, vocabulary,
                Integer.parseInt(properties.getProperty("query.numKeywords")),
                Integer.parseInt(properties.getProperty("query.numResults")),
                Integer.parseInt(properties.getProperty("query.numQueries")),
                0.0,
                Double.parseDouble(properties.getProperty("dataset.spaceMaxValue")),
                properties.getProperty("query.type"),
                properties.getProperty("query.keywords"),
                Integer.parseInt(properties.getProperty("query.numMostFrequentTerms")),
                Integer.parseInt(properties.getProperty("query.numWarmUpQueries")),
                prefix + "/ifList",
                Integer.parseInt(properties.getProperty("if.cacheSize")),
                prefix + "/" + properties.getProperty("if.docLengthFile"),
                prefix + "/" + properties.getProperty("if.termInfoFile"),
                Integer.parseInt(properties.getProperty("query.neighborhood")),
                rTree,
                Double.parseDouble(properties.getProperty("query.radius")));

        searching.open();
        searching.run();
        searching.close();

        System.out.println("\n\nStatistics:\n" + statistics.getStatus());

    }
}
