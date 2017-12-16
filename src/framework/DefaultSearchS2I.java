/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package framework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import util.RandomUtil;
import util.Util;
import util.experiment.Experiment;
import util.experiment.ExperimentException;
import util.experiment.ExperimentResult;
import util.sse.SSEExeption;
import util.sse.Vocabulary;
import util.statistics.StatisticCenter;
import xxl.core.spatial.points.DoublePoint;
import xxl.core.spatial.points.Point;

/**
 *
 * @author joao
 */
public abstract class DefaultSearchS2I implements Experiment {
    private HashMap<Integer, TermEntry> termHash;
    protected final Vocabulary termVocabulary;
    protected final StatisticCenter statisticCenter;
    protected ArrayList<ExperimentResult> result;

    private final int numQueries;
    
    protected final double spaceMaxValue;

    private final double spaceMaxDistance;

    private int datasetSize=-1;
    protected final int numKeywords;
    protected final int numResults;
    protected final double alpha;

    private final boolean randomQueries;
    private final boolean debugMode;
    private boolean warmUp;
    private final int numWarmUpQueries;
    private int iterations;

    public DefaultSearchS2I(StatisticCenter statisticCenter, boolean debugMode, Vocabulary termVocabulary,
            int numKeywords, int numResults, int numQueries, double alpha, 
            double spaceMaxValue, boolean randomQueries, int numWarmUpQueries){
        this.termVocabulary = termVocabulary;
        this.statisticCenter = statisticCenter;
        this.numKeywords = numKeywords;
        this.numResults = numResults;
        this.numQueries = numQueries;
        this.alpha = alpha;
        this.spaceMaxValue = spaceMaxValue;
        this.debugMode = debugMode;
        this.randomQueries = randomQueries;
        this.numWarmUpQueries = numWarmUpQueries;


        //The max distance is the diagonal of the square whose sides are spaceMaxValue
        this.spaceMaxDistance = Math.sqrt(spaceMaxValue*spaceMaxValue+ spaceMaxValue*spaceMaxValue);
    }


    public void open() throws ExperimentException {
        termHash = new HashMap<Integer, TermEntry>();
        int termId;
        int df;
        for(Entry<String,Integer> entry:termVocabulary.entrySet()){
            termId=entry.getValue();
            df = getDocumentFrequency(termId);
            if(df>0){
                termHash.put(termId, new TermEntry(entry.getKey(), df));
            }
        }

        if(numWarmUpQueries>0){
            warmUp = true;
            iterations = numWarmUpQueries; 
        }else{
            warmUp=false;
            iterations = numQueries;
        }
    }

    public final void run() throws ExperimentException {
        try{
            long time;
            String queryKeywords;
            Point point;
            result = null;
            Iterator<SpatioTextualObject> spatialObjects=null;

            if(debugMode && warmUp) System.out.println("\n[WarmUp, queries="+iterations+"]");

            for(int i=0;i<iterations;i++){
                point= nextQueryPoint();
                if(randomQueries){
                    queryKeywords = randomQueryKeywords(numKeywords);
                    if(debugMode) System.out.print("[RandomQueries]");
                }else{
                    if(debugMode) System.out.print("[MostFrequentQueries]");
                    queryKeywords = mostFrequentQueryKeywords(numKeywords);
                }

               // point = new DoublePoint(new double[]{6,5});
               // queryKeywords = "bar pub";

                if(debugMode){
                    System.out.print("# query "+(i+1)+"/"+iterations+" = ['"+queryKeywords+"'("+
                        Util.cast(point.getValue(0),3)+","+Util.cast(point.getValue(1),3)+")... ");
                }
                                
                time = System.currentTimeMillis();

                spatialObjects = execute(point.getValue(0), point.getValue(1), 
                        spaceMaxDistance, queryKeywords, numResults, alpha);
                
                if(debugMode) System.out.println(Util.time(System.currentTimeMillis()-time)+"]");

                 if(debugMode){
                     result = new ArrayList();
                     for(int v=0; spatialObjects.hasNext(); v++){
                        result.add(spatialObjects.next());
                        System.out.println("-->["+(v+1)+"]  " +result.get(v).toString());
                     }
                 }
            }

            if(warmUp){
                warmUp=false;
                iterations = numQueries;
                statisticCenter.resetStatistics();
                if(debugMode) System.out.println("\n[For real! queries="+numQueries+"]");
                run();
            }

            collectStatistics(iterations);

            if(result==null){
                result = new ArrayList();
                for(int i=0; spatialObjects.hasNext(); i++){
                    result.add(spatialObjects.next());
                }
            }
        }catch(Exception e){
            throw new ExperimentException(e);
        }
    }

    protected abstract void collectStatistics(int numQueries);

    /**
     * Returns the number of documents in which the term appears.
     * @param termId
     * @return
     */
    protected abstract int getDocumentFrequency(int termId);

    /**
     * Returns the total number of objects in the dataset
     * @return
     */
    protected abstract int getTotalNumObjects();


    /**
     * Implement the query processing
     * @param queryLatitude
     * @param queryLongitude
     * @param queryVector
     * @param queryLengt the total number of terms in the query
     * @param k
     * @param alpha
     * @return
     * @throws ExperimentException
     */
    protected abstract  Iterator<SpatioTextualObject> execute(double queryLatitude,
            double queryLongitude, double maxDist,  String queryKeywords,
            int k, double alpha) throws ExperimentException;


    public ExperimentResult[] getResult() {
        return result.toArray(new ExperimentResult[result.size()]);
    }

    /**
     * Produces a random point in the spaceMaxValue
     * @return
     */
    private Point nextQueryPoint() {
        return new DoublePoint(new double[]{RandomUtil.nextDouble(spaceMaxValue),
                                            RandomUtil.nextDouble(spaceMaxValue)});
    }


    private String mostFrequentQueryKeywords(int numKeywords) throws SSEExeption{
        String[] keywords = new String[numKeywords];
        TermEntry entry;
        double termProbability;        
        datasetSize = datasetSize==-1 ? getTotalNumObjects() : datasetSize;
        for(int i=0;i<numKeywords;i++){
            do{
                while((entry = termHash.get(RandomUtil.nextInt(termHash.size())))==null);
                termProbability=entry.getDocumentFrequency()/(double)datasetSize;
            }while(RandomUtil.nextDouble(1)>termProbability);
            keywords[i] = entry.getTerm();
        }        
        return Arrays.toString(keywords);
    }

    private String randomQueryKeywords(int numKeywords) {
        String[] keywords = new String[numKeywords];
        for(int i=0;i<numKeywords;i++){
            keywords[i] = termHash.get(RandomUtil.nextInt(termHash.size())).getTerm();
        }
        return Arrays.toString(keywords);
    }

    class TermEntry{
        private final String term ;
        private final int docFrequency;

        public TermEntry(String term, int docFrequency){
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
        public String toString(){
            return term+"="+docFrequency;
        }
    }
}
