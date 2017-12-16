package s2i.preference;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import s2i.SpatialInvertedIndex;
import s2i.SpatioItem;
import util.DefaultSearch;
import util.ScoredObject;
import util.cache.MinHeap;
import util.file.ColumnFileException;
import util.file.DataNotFoundException;
import util.nra.Source;
import util.sse.SSEExeption;
import util.sse.Term;
import util.sse.Vector;
import util.statistics.StatisticCenter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.Rectangle;

public class PreferenceIndex extends SpatialInvertedIndex {

    public PreferenceIndex(StatisticCenter statisticCenter, String outputPath,
            int blockSize, int treesOppened, PreferenceFileTermManager fileTermManager,
            PreferenceTreeTermManager treeTermManager, boolean constructionTime) {
        super(statisticCenter, outputPath, blockSize, treesOppened,
                fileTermManager, treeTermManager, constructionTime);
    }

   
    /**
     * The score of the objects is the textual of the nearest feature
     */
    public void nnQuery(ScoredObject p, Vector queryVector, double queryLength) throws SSEExeption,
            IOException, ClassNotFoundException, ColumnFileException, DataNotFoundException {

        DoublePoint queryLocation = new DoublePoint(new double[]{p.getLatitude(), p.getLongitude()});
        double minDistance = Double.MAX_VALUE;

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getSourceNN(queryTerm, queryLocation, queryLength);
                } else {
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getSourceNN(queryTerm, queryLocation, queryLength);
                }

            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext() {
                        return false;
                    }

                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        FeatureEntry entry = nextEntry(heap);

        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            while (other != null && entry.feature.getId() == other.feature.getId()) {
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }

            minDistance = updateScoreNN(p, entry, minDistance);

            entry = other;
        }
        if (entry != null) {
            minDistance = updateScoreNN(p, entry, minDistance);
        }
    }

    /**
     * The score of the objects is the textual of the feature at range
     */
    public void rangeQuery(ScoredObject p, Vector queryVector, double queryLength, double radius) throws SSEExeption,
            IOException, ClassNotFoundException, ColumnFileException, DataNotFoundException {

        DoublePoint queryLocation = new DoublePoint(new double[]{p.getLatitude(), p.getLongitude()});

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getSourceRange(queryTerm, queryLength, queryLocation, radius);
                } else {
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getSourceRange(queryTerm, queryLength, queryLocation, radius);
                }

            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext() {
                        return false;
                    }

                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);
        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            //While the features have the same id
            while (other != null && entry.feature.getId() == other.feature.getId()) {
                //update the score of the entry
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }
            //Update the score of V, based on the feature
            updateScore(p, entry, radius);

            entry = other;
        }
        if (entry != null) {
            updateScore(p, entry, radius);
        }
    }
    
    void influenceQuery(ScoredObject p, Vector queryVector, double queryLength, double radius)
            throws SSEExeption, IOException, ClassNotFoundException, ColumnFileException {

        DoublePoint queryLocation = new DoublePoint(new double[]{p.getLatitude(), p.getLongitude()});

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getSourceInfluence(queryTerm, queryLength, queryLocation, radius);
                } else {
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getSourceInfluence(queryTerm, queryLength, queryLocation, radius);
                }

            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext() {
                        return false;
                    }

                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);
        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            //While the features have the same id
            while (other != null && entry.feature.getId() == other.feature.getId()) {
                //update the score of the entry
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }
            
            influenceScore(p, entry, radius);            

            entry = other;
        }
        
        if (entry != null) {
            influenceScore(p, entry, radius);
        }
    }

    void nnQueryGroup(Rectangle mbr, List<ScoredObject> list, Vector queryVector, double queryLength)
            throws SSEExeption, IOException, ClassNotFoundException, ColumnFileException {

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;

        //atualiza feature mais próximo dentro do conjunto V
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getGroupSourceNN(mbr, list, queryTerm, queryLength);
                } else {
//                    System.out.println("FILE");
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getGroupSourceNN(mbr, list, queryTerm, queryLength);
                }

            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext() {
                        return false;
                    }

                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }


        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);
        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            //While the features have the same id
            while (other != null && entry.feature.getId() == other.feature.getId()) {
                //update the score of the feature                
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }
            //Update the score of V, based on the feature
            updateScoreNNGroup(list, entry);

            entry = other;
        }
        if (entry != null) {          
            updateScoreNNGroup(list, entry);
        }
    }

    void rangeQueryGroup(Rectangle mbr, List<ScoredObject> list,
            Vector queryVector, double queryLength, double radius) throws SSEExeption, ColumnFileException, IOException, ClassNotFoundException {

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    //System.out.println("Tree");
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getGroupSourceRange(queryTerm, queryLength, mbr, radius);
                } else {
                    //System.out.println("File");
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getGroupSourceRange(queryTerm, queryLength, mbr, radius);
                }

            } else {
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);
        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            //While the features have the same id
            while (other != null && entry.feature.getId() == other.feature.getId()) {
                //update the score of the feature                
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }
            //Update the score of V, based on the feature
            updateScore(list, entry, radius);

            entry = other;
        }
        if (entry != null) {
            updateScore(list, entry, radius);
        }
    }

    private FeatureEntry nextEntry(MinHeap<FeatureEntry> heap) {
        FeatureEntry entry = heap.poll();
        if (entry != null && entry.source.hasNext()) {
            heap.add(new FeatureEntry(entry.source.next(), entry.source));
        }
        return entry;
    }

    private static void updateScore(ScoredObject p, FeatureEntry entry, double radius) {
 
        if ((entry.getScore() > p.getScore())) {
            p.setScore(entry.getScore());
        }
    }

    private static void influenceScore(ScoredObject p, FeatureEntry entry, double radius) {
//        
        double distanceToFeature = DefaultSearch.euclideanDistance(p.getLatitude(), p.getLongitude(),
                entry.feature.getLatitude(), entry.feature.getLongitude());                
        
        double influenceScore = entry.getScore() * Math.pow(2, -distanceToFeature / radius);
        
        if (influenceScore > p.getScore()) {            
            p.setScore(influenceScore);
        }
    }

    private static void updateScore(List<ScoredObject> objects, FeatureEntry entry, double radius) {
        for (ScoredObject p : objects) {//remover condição
            if ((DefaultSearch.euclideanDistance(p.getLatitude(), p.getLongitude(),
                    entry.feature.getLatitude(), entry.feature.getLongitude()) < radius) && (entry.getScore() > p.getScore())) {
                p.setScore(entry.getScore());
            }
        }
    }

    private static double updateScoreNN(ScoredObject p, FeatureEntry entry, double minDistance) {

        double distance = entry.feature.getDistancia();   
        
        if (distance < minDistance) {

            minDistance = distance;
            p.setScore(entry.getScore());

        } else if (distance == minDistance && entry.getScore() > p.getScore()) {
            p.setScore(entry.getScore());
        }

        return minDistance;
    }

    private static void updateScoreNNGroup(List<ScoredObject> objects, FeatureEntry entry) {

        for (ScoredObject p : objects) {

            double dist = DefaultSearch.euclideanDistance(p.getLatitude(), p.getLongitude(),
                    entry.feature.getLatitude(), entry.feature.getLongitude());

            if (dist < p.getDistancia()) {
                p.setScore(entry.getScore());
                p.setDistancia(dist);
            } else if (entry.getScore() > p.getScore() && dist == p.getDistancia()) {
                p.setScore(entry.getScore());
            }
        }
    }

    private static void updateInfluenceScore(List<ScoredObject> objects, FeatureEntry entry, double radius) {

        for (ScoredObject p : objects) {

            DoublePoint object = new DoublePoint(new double[]{p.getLatitude(), p.getLongitude()});

            double distToP = object.distanceTo(new DoublePoint(new double[]{entry.feature.getLatitude(), entry.feature.getLongitude()}));
            double score = entry.getScore() * Math.pow(2, -distToP / radius);

            if (score > p.getScore()) {
                p.setScore(score);
            }
        }
    }

    void influenceQueryGroup(Rectangle mbr, List<ScoredObject> list, Vector queryVector, double queryLength, double radius)
            throws IOException, ClassNotFoundException, ColumnFileException, SSEExeption {

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm = null;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    //System.out.println("Tree");
                    sources[i] = ((PreferenceTreeTermManager) treeTermManager).getGroupSourceInfluence(queryTerm, queryLength, mbr);
                } else {
                    //System.out.println("File");
                    sources[i] = ((PreferenceFileTermManager) fileTermManager).getGroupSourceInfluence(queryTerm, queryLength);
                }

            } else {
                System.out.println("\nterm='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not found!");
            }
        }

        MinHeap<FeatureEntry> heap = new MinHeap<>();

        //Stores the first feature of each list in the heap. Each heap entry has a pointer to its source
        for (int i = 0; i < sources.length; i++) {
            if (sources[i].hasNext()) {
                heap.add(new FeatureEntry(sources[i].next(), sources[i]));
            }
        }

        //Get the entry with smallest id
        FeatureEntry entry = nextEntry(heap);
        while (entry != null && !heap.isEmpty()) {
            FeatureEntry other = nextEntry(heap);

            //While the features have the same id
            while (other != null && entry.feature.getId() == other.feature.getId()) {
                //update the score of the feature                
                entry.incScore(other.getScore());
                other = nextEntry(heap);
            }
            //Update the score of V, based on the feature
            updateInfluenceScore(list, entry, radius);

            entry = other;
        }
        if (entry != null) {
            updateInfluenceScore(list, entry, radius);
        }
    }

    private class FeatureEntry implements Comparable {

        final Source<SpatioItem> source;
        final SpatioItem feature;
        double score = 0;

        public FeatureEntry(SpatioItem feature, Source<SpatioItem> source) {
            this.feature = feature;
            this.source = source;
            this.score = feature.getScore();
        }

        @Override
        public int compareTo(Object o) {
            FeatureEntry other = (FeatureEntry) o;
            return feature.getId() - other.feature.getId();
        }

        public void incScore(double partialScore) {
            this.score += partialScore;
        }

        public double getScore() {
            return score;
        }
    }
}
