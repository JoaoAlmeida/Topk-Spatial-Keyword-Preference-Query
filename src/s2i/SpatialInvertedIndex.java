/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;


import util.SpatioTextualObject;
import java.io.IOException;
import java.util.ArrayList;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import util.file.BufferedListStorage;
import util.file.ColumnFileException;
import util.file.DataNotFoundException;
import util.file.PersistentHashMap;
import util.file.EntryStorage;
import util.file.IntegerEntry;
import util.file.MetadataStorage;
import util.nra.NRA;
import util.nra.Source;
import util.sse.SSEExeption;
import util.sse.Term;
import util.sse.Vector;
import util.sse.Vocabulary;
import util.statistics.StatisticCenter;
import xxl.util.MaxDoubleRTree;
import xxl.util.MaxDoubleRectangle;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.rectangles.DoublePointRectangle;

/**
 *
 * @author joao
 */
public class SpatialInvertedIndex {
    //Vocabulary that maps terms to an id.

    protected Vocabulary termVocabulary;
    //maps term id (from termVocabulary) to the number of documents that has the term (document frequency of the term)
    protected EntryStorage<IntegerEntry> termInfo;
    protected MetadataStorage<IntegerEntry> info;
    private final StatisticCenter statisticCenter;
    private final String outputPath;
    //Maps term_id:termType, the type can be FILE_TYPE or TREE_TYPE;
    protected static final int FILE_TYPE = 0; //Items managed by the FileTermManager
    protected static final int TREE_TYPE = 1; //Items managed by the TreeTermManaagerCache
    protected static final int NEAREST_QUERY = 0;
    protected static final int RANGE_QUERY = 1;
    protected static final int INFLUENCE_QUERY = 2;
    protected final PersistentHashMap<Integer, Integer> termType;
    protected final FileTermManager fileTermManager;
    protected final TreeTermManager treeTermManager;
    private BufferedListStorage<SpatioItem> fileTreeStore; //Used only in constructionTime
    private final int blockCapacity; //Stores the capacity (number of objects) that can be stored in a block
    private final boolean constructionTime;
    private int dataSetSize = -1;

    public SpatialInvertedIndex(StatisticCenter statisticCenter, String outputPath,
            int blockSize, int treesOppened, FileTermManager fileTermManager,
            TreeTermManager treeTermManager, boolean constructionTime) {
        this.fileTermManager = fileTermManager;
        this.treeTermManager = treeTermManager;

        this.statisticCenter = statisticCenter;
        this.outputPath = outputPath;
        this.termType = new PersistentHashMap<Integer, Integer>(outputPath + "/termType.obj");

        this.blockCapacity = blockSize / SpatioItem.SIZE;
        this.constructionTime = constructionTime;

        if (constructionTime) {
            fileTreeStore = new BufferedListStorage<SpatioItem>(statisticCenter,
                    "fileTreeStore", outputPath + "/tmpFiles", treesOppened,
                    SpatioItem.SIZE, SpatioItem.FACTORY);
        }
    }

    public EntryStorage<IntegerEntry> getTermInfo(){
        return termInfo;
    }
    
    public void open() throws SSEExeption, ColumnFileException, IOException, ClassNotFoundException {
        termType.open();

        termVocabulary = new Vocabulary(outputPath + "/termVocabulary.txt");
        termVocabulary.open();

        termInfo = new EntryStorage<IntegerEntry>(statisticCenter, "termInfo",
                outputPath + "/termInfo", IntegerEntry.SIZE, IntegerEntry.FACTORY);
        termInfo.open();

        info = new MetadataStorage<IntegerEntry>(outputPath + "/info", IntegerEntry.SIZE, IntegerEntry.FACTORY);
        info.open();

        fileTermManager.open();

        if (constructionTime) {
            fileTreeStore.open();
        } else {
            //Open in the method buildTrees.
            treeTermManager.open();
        }
    }

    public boolean isConstructionTime() {
        return constructionTime;
    }

    public void insert(int id, double latitude, double longitude, String text) throws SSEExeption, IOException, ColumnFileException, ClassNotFoundException {
        Vector vector = new Vector(id);
        int numWords = Vector.vectorize(vector, text, termVocabulary);

        statisticCenter.getCount("totalNumberOfWords").update(numWords);
        statisticCenter.getTally("avgNumDistinctTerms").update(vector.size());
        insert(id, latitude, longitude, vector);
    }

    public void insert(int id, double latitude, double longitude, Vector vector) throws SSEExeption, IOException, ColumnFileException, ClassNotFoundException {
        dataSetSize = -1; //Force update during execution of method getDataSetSize()

        double docLength = Vector.computeDocWeight(vector);

        IntegerEntry entry;
        Iterator<Term> terms = vector.iterator();
        Term term;
        Integer type;
        while (terms.hasNext()) {
            term = terms.next();

            //We normalize weight by the document lenght, which gives the impact of t
            term.setWeight(term.getWeight() / docLength);
            List<SpatioItem> list;
            type = termType.get(term.getTermId());           
            if (type == null || type == FILE_TYPE) {
                list = fileTermManager.getList(term.getTermId(), false);
                if (list == null) {
                    list = new ArrayList<SpatioItem>();                    
                }

                list.add(new SpatioItem(id, latitude, longitude, term.getWeight()));
                if (list.size() > blockCapacity) { //moves all terms stored in the file to the tree
                    termType.put(term.getTermId(), TREE_TYPE);
                    fileTermManager.remove(term.getTermId());
                    insertTree(term.getTermId(), list);
                } else {
                    termType.put(term.getTermId(), FILE_TYPE);
                    fileTermManager.putList(term.getTermId(), list, false);
                }
            } else {
                list = new ArrayList<SpatioItem>(1);
                list.add(new SpatioItem(id, latitude, longitude, term.getWeight()));
                insertTree(term.getTermId(), list);
            }

            //Update the df (document frequency) of the term
            entry = termInfo.getEntry(term.getTermId());
            termInfo.putEntry(term.getTermId(), entry == null ? new IntegerEntry(1)
                    : new IntegerEntry(entry.getValue() + 1));
        }

        //Update the datasetSize
        entry = info.getEntry();
        info.putEntry(entry == null ? new IntegerEntry(1)
                : new IntegerEntry(entry.getValue() + 1));
    }

    private void insertTree(int termId, List<SpatioItem> newList) throws IOException, ClassNotFoundException, ColumnFileException {
        if (constructionTime) {
            fileTreeStore.addList(termId, newList, false);
        } else {
            MaxDoubleRTree tree = treeTermManager.get(termId);
            insert(tree, newList);
        }
    }

    private void insert(MaxDoubleRTree tree, List<SpatioItem> list) {
        for (SpatioTextualObject entry : list) {
            tree.insert(new KPE(
                    new MaxDoubleRectangle(
                    new double[]{entry.getLatitude(), entry.getLongitude()},
                    new double[]{entry.getLatitude(), entry.getLongitude()},
                    entry.getScore()),
                    entry.getId(), IntegerConverter.DEFAULT_INSTANCE));
        }
    }

    public Iterator<SpatioTextualObject> query(double[] minCoordinate,
            /*REMOVER*/ double[] maxCoordinate, String queryKeywords) throws SSEExeption,
            IOException, ClassNotFoundException, ColumnFileException, DataNotFoundException {

        Vector queryVector = new Vector();
        Vector.vectorize(queryVector, queryKeywords, termVocabulary);
        double queryLength = Vector.computeQueryWeight(queryVector, getDatasetSize(), termInfo);

        SpatioTextualObject resultItem, item;
        final LinkedHashMap<Integer, SpatioTextualObject> resultSet;
        resultSet = new LinkedHashMap<Integer, SpatioTextualObject>();

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm;
        Integer type = null;
        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = treeTermManager.getSourceMBR(queryTerm,
                            new DoublePointRectangle(new DoublePoint(minCoordinate),
                            new DoublePoint(maxCoordinate)));
                } else {
                    sources[i] = fileTermManager.getSourceMBR(queryTerm,
                            new DoublePointRectangle(new DoublePoint(minCoordinate),
                            new DoublePoint(maxCoordinate)));
                }

            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext(){
                        return false;
                    }
                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("term='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not indexed!");
            }

            //Aggregate the scores of the objects received from different sources
            while ((item = sources[i].next()) != null) {
                resultItem = resultSet.get(item.getId());
                if (resultItem == null) {
                    resultItem = new SpatioItem(item.getId(), item.getLatitude(),
                            item.getLongitude(), 0);
                    resultSet.put(item.getId(), resultItem);
                }
                ((SpatioItem) resultItem).setScore(resultItem.getScore()
                        + item.getScore()/*TermImpact()*/ * (queryTerm.getWeight() / queryLength));
            }
        }

        return resultSet.values().iterator();
    }

    public Iterator<SpatioTextualObject> search(double latitude,
            double longitude, double maxDist, String queryKeywords, int k, double alpha) throws SSEExeption,
            IOException, ClassNotFoundException, ColumnFileException, DataNotFoundException {

        Vector queryVector = new Vector();
        Vector.vectorize(queryVector, queryKeywords, termVocabulary);
        double queryLength = Vector.computeQueryWeight(queryVector, getDatasetSize(), termInfo);

        Source<SpatioItem>[] sources = new Source[queryVector.size()];

        Iterator<Term> it = queryVector.iterator();
        Term queryTerm;
        Integer type = null;

        for (int i = 0; it.hasNext(); i++) {
            queryTerm = it.next();

            type = termType.get(queryTerm.getTermId());
            if (type != null) {
                if (type == TREE_TYPE) {
                    sources[i] = treeTermManager.getSource(queryTerm, queryVector.size(), queryLength,
                            new DoublePoint(new double[]{latitude, longitude}), maxDist, alpha);
                } else {
                    sources[i] = fileTermManager.getSource(queryTerm, queryVector.size(), queryLength,
                            new DoublePoint(new double[]{latitude, longitude}), maxDist, alpha);
                }
            } else {
                sources[i] = new Source<SpatioItem>() {
                    public boolean hasNext(){
                        return false;
                    }
                    public SpatioItem next() {
                        return null;
                    }
                };
                System.out.println("term='" + termVocabulary.getTerm(queryTerm.getTermId()) + "' was not indexed!");
            }
        }

        NRA<SpatioItem> nra = new NRA<SpatioItem>(sources, k);
        nra.run();

        final Iterator<SpatioItem> result = nra.getResult();

        return new Iterator<SpatioTextualObject>() {
            public boolean hasNext() {
                return result.hasNext();
            }

            public SpatioTextualObject next() {
                return result.next();
            }

            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }

    /**
     * @param STATUS_TIME
     * @return the number of trees constructed.
     * @throws ColumnFileException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public int buildTrees(long STATUS_TIME) throws ColumnFileException, IOException, ClassNotFoundException {
        treeTermManager.open();
        long time = System.currentTimeMillis();
        int count = 0;
        fileTreeStore.resetCacheSize(0);
        List<Integer> ids = fileTreeStore.getIds();
        int totalSize = ids.size();
        for (int termId : ids) {
//            System.out.println(termId);
            MaxDoubleRTree tree = treeTermManager.get(termId);
            insert(tree, fileTreeStore.getList(termId));
            count++;
            if ((System.currentTimeMillis() - time) > STATUS_TIME) {
                time = System.currentTimeMillis();
                System.out.print("[" + count + "/" + totalSize + "] ");
            }
        }
        System.out.println("\nRemoving the temporary file created during indexing...");
        fileTreeStore.delete();
        return count;
    }
    
    /**
     * Score of a term
     *
     * @return
     */
    public static double textualPartialScore(double queryWeight, double queryLenght, double docTermWeight) {

        double textRelevance = docTermWeight * (queryWeight / queryLenght); // X = queryTerm.getWeight()/ Wtqd = Vector.computeQueryWeight();

        //return (1 - alpha) * textRelevance;
        return textRelevance;
    }

    public static double spatioPartialScore(int queryKeywords,
            double distanceToQueryPoint, double maxDistance, double alpha) {
        double spatialProximity = 1 - (distanceToQueryPoint / maxDistance);

        return alpha * spatialProximity / (double) queryKeywords;
    }

    /**
     * @return the termVocabulary
     */
    public Vocabulary getTermVocabulary() {
        return termVocabulary;
    }

    public int getDocumentFrequency(int termId) throws SSEExeption, IOException, ColumnFileException, DataNotFoundException {
        IntegerEntry entry = termInfo.getEntry(termId);
        return entry==null? -1 : termInfo.getEntry(termId).getValue();
    }

    public long getNumDistinctTerms() {
        return termVocabulary.size();
    }

    public int getDatasetSize() throws SSEExeption, IOException, ColumnFileException, DataNotFoundException {
        if (dataSetSize == -1) {
            IntegerEntry entry = info.getEntry();
            dataSetSize = entry == null ? 0 : entry.getValue();
        }
        return dataSetSize;
    }

    public long getFileManagerSizeInBytes() throws ColumnFileException, IOException, DataNotFoundException {
        return fileTermManager.getSizeInBytes();
    }

    public long getTreeManagerSizeInBytes() {
        return treeTermManager.getSizeInBytes();
    }

    public long getSizeInBytes() throws ColumnFileException, DataNotFoundException, IOException {
        return fileTermManager.getSizeInBytes() + treeTermManager.getSizeInBytes();
    }

    public void flush() throws ColumnFileException, IOException {
        fileTermManager.flush();
        treeTermManager.flush();
    }

    public void close() throws SSEExeption, ColumnFileException, IOException {
        termVocabulary.close();
        termInfo.close();

        info.close();

        termType.close();

        fileTermManager.close();

        treeTermManager.close();
    }
}
