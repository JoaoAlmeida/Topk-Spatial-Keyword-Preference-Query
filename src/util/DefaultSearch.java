/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import util.cache.MinHeap;
import util.experiment.Experiment;
import util.experiment.ExperimentException;
import util.experiment.ExperimentResult;
import util.sse.SSEExeption;
import util.sse.Vocabulary;
import util.statistics.StatisticCenter;
import xxl.core.spatial.LpMetric;
import xxl.core.spatial.points.DoublePoint;

/**
 *
 * @author joao
 */
public abstract class DefaultSearch implements Experiment {

    private HashMap<Integer, TermEntry> mostFrequent;
    private HashMap<Integer, TermEntry> termHash;
    protected final Vocabulary termVocabulary;
    protected final StatisticCenter statisticCenter;
    protected ArrayList<ExperimentResult> result;
    private final int numQueries;
    protected final double spaceMaxValue;
    protected final double spaceMaxDistance;
    private int datasetSize = -1;
    protected final int numKeywords;
    protected final int numResults;
    protected final double alpha;
    private final String queryType;
    public final boolean debugMode;
    private boolean warmUp;
    private final int numWarmUpQueries;
    private final int numMostFrequentTerms;
    private int iterations;
    private final String customKeywords;

    public DefaultSearch(StatisticCenter statisticCenter, boolean debugMode, Vocabulary termVocabulary,
            int numKeywords, int numResults, int numQueries, double alpha,
            double spaceMaxValue, String queryType, String customKeywords, int numMostFrequentTerms, int numWarmUpQueries) {
        this.termVocabulary = termVocabulary;
        this.statisticCenter = statisticCenter;
        this.numKeywords = numKeywords;
        this.numResults = numResults;
        this.numQueries = numQueries;
        this.alpha = alpha;
        this.spaceMaxValue = spaceMaxValue;
        this.debugMode = debugMode;
        this.queryType = queryType;
        this.customKeywords = customKeywords;
        this.numWarmUpQueries = numWarmUpQueries;
        this.numMostFrequentTerms = numMostFrequentTerms;

        //The max distance is the diagonal of the square whose sides are spaceMaxValue
        this.spaceMaxDistance = Math.sqrt(spaceMaxValue * spaceMaxValue + spaceMaxValue * spaceMaxValue);
    }

    @Override
    public void open() throws ExperimentException {
        termHash = new HashMap<>();
        MinHeap<TermEntry> heap = new MinHeap<>();

        int termId;
        int df;
        for (Entry<String, Integer> entry : termVocabulary.entrySet()) {
            termId = entry.getValue();
            df = getDocumentFrequency(termId);
            if (df > 0) {
                TermEntry termEntry = new TermEntry(entry.getKey(), df);
                termHash.put(termId, termEntry);
                heap.add(termEntry);
                if (heap.size() > numMostFrequentTerms) {
                    heap.remove();
                }
            }
        }

        mostFrequent = new HashMap<>(numMostFrequentTerms);
        for (int i = 0; i < numMostFrequentTerms; i++) {
            TermEntry termEntry = heap.poll();
            mostFrequent.put(i, termEntry);
        }

        if (numWarmUpQueries > 0) {
            warmUp = true;
            iterations = numWarmUpQueries;
        } else {
            warmUp = false;
            iterations = numQueries;
        }
    }

    @Override
    public final void run() throws ExperimentException {
        try {
            long time;
            String queryKeywords;
            DoublePoint point;
            result = null;
            Iterator<SpatioTextualObject> spatialObjects = null;

            if (debugMode && warmUp) {
                System.out.println("\n[WarmUp, queries=" + iterations + "]");
            }

            for (int i = 0; i < iterations; i++) {
                point = nextQueryPoint();
                if (queryType.equals("random")) {
                    queryKeywords = randomQueryKeywords(numKeywords);
                    if (debugMode) {
                        System.out.print("[RandomQueries]");
                    }
                } else if (queryType.equals("mostFrequent")) {
                    if (debugMode) {
                        System.out.print("[MostFrequentQueries]");
                    }
                    queryKeywords = mostFrequentQueryKeywords(numKeywords);
                } else if (queryType.equals("custom")) {
                    if (debugMode) {
                        System.out.print("[CustomQueries]");
                    }
                    queryKeywords = customKeywords;
                } else {
                    throw new ExperimentException("query.type=" + queryType + " is invalid!");
                }

                // point = new DoublePoint(new double[]{6,5});
                // queryKeywords = "bar pub";

                if (debugMode) {
                    System.out.print("# query " + (i + 1) + "/" + iterations + " = ['" + queryKeywords + "'("
                            + Util.cast(point.getValue(0), 3) + "," + Util.cast(point.getValue(1), 3) + ")... ");
                }

                time = System.currentTimeMillis();

                spatialObjects = execute(point.getValue(0), point.getValue(1),
                        spaceMaxDistance, queryKeywords, numResults, alpha);

                if (debugMode) {
                    System.out.println(Util.time(System.currentTimeMillis() - time) + "]");
                }

                if (debugMode) {
                    result = new ArrayList();
                    for (int v = 0; spatialObjects.hasNext(); v++) {
                        result.add(spatialObjects.next());
                        System.out.println("-->[" + (v + 1) + "]  " + result.get(v).toString());
                    }
                }
            }

            if (warmUp) {
                warmUp = false;
                iterations = numQueries;
                statisticCenter.resetStatistics();
                if (debugMode) {
                    System.out.println("\n[For real! queries=" + numQueries + "]");
                }
                run();
            }

            collectStatistics(iterations);

            if (result == null) {
                result = new ArrayList();
                for (int i = 0; spatialObjects.hasNext(); i++) {
                    result.add(spatialObjects.next());
                }
            }
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    protected abstract void collectStatistics(int numQueries);

    /**
     * Returns the number of documents in which the term appears.
     *
     * @param termId
     * @return
     */
    protected abstract int getDocumentFrequency(int termId);

    /**
     * Returns the total number of objects in the dataset
     *
     * @return
     */
    protected abstract int getTotalNumObjects();

    /**
     * Implement the query processing
     *
     * @param queryLatitude
     * @param queryLongitude
     * @param queryVector
     * @param queryLengt the total number of terms in the query
     * @param k
     * @param alpha
     * @return
     * @throws ExperimentException
     */
    protected abstract Iterator<SpatioTextualObject> execute(double queryLatitude,
            double queryLongitude, double maxDist, String queryKeywords,
            int k, double alpha) throws ExperimentException;

    public ExperimentResult[] getResult() {
        return result.toArray(new ExperimentResult[result.size()]);
    }

    public static double euclideanDistance(double p1Lat, double p1Lgt, double p2Lat, double p2Lgt) {
        return LpMetric.EUCLIDEAN.distance(
                new DoublePoint(new double[]{p1Lat, p1Lgt}),
                new DoublePoint(new double[]{p2Lat, p2Lgt}));
    }

    /**
     * Produces a random point in the spaceMaxValue
     *
     * @return
     */
    private DoublePoint nextQueryPoint() {
        return new DoublePoint(new double[]{RandomUtil.nextDouble(spaceMaxValue),
            RandomUtil.nextDouble(spaceMaxValue)});
    }

    private String mostFrequentQueryKeywords(int numKeywords) throws SSEExeption {
        String[] keywords = new String[numKeywords];
        datasetSize = datasetSize == -1 ? getTotalNumObjects() : datasetSize;

        String keyword = null;
        for (int i = 0; i < numKeywords; i++) {
            keyword = mostFrequent.get(RandomUtil.nextInt(numMostFrequentTerms)).getTerm();
            if (!contains(keywords, keyword)) {
                keywords[i] = keyword;
            } else {
                i--;
            }
        }
        return Arrays.toString(keywords);
    }

    private static final boolean contains(String[] array, String key) {
        if (array != null && array.length < 0) {
            for (String str : array) {
                if (str.equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    private String randomQueryKeywords(int numKeywords) {
        String[] keywords = new String[numKeywords];
        for (int i = 0; i < numKeywords; i++) {
            keywords[i] = termHash.get(RandomUtil.nextInt(termHash.size())).getTerm();
        }
        return Arrays.toString(keywords);
    }

    class TermEntry implements Comparable {

        private final String term;
        private final int docFrequency;

        public TermEntry(String term, int docFrequency) {
            this.term = term;
            this.docFrequency = docFrequency;
        }

        public String getTerm() {
            return term;
        }

        public int getDocumentFrequency() {
            return docFrequency;
        }

        @Override
        public String toString() {
            return term + "=" + docFrequency;
        }

        @Override
        public int compareTo(Object o) {
            return docFrequency - ((TermEntry) o).docFrequency;
        }
    }
}
