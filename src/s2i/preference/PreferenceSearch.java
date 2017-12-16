/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i.preference;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import util.DefaultSearch;
import util.LoadRTree;
import util.ScoredObject;
import util.SpatioTextualObject;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import s2i.SpatioTreeHeapEntry;
import util.config.Settings;
import util.experiment.ExperimentException;
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
public class PreferenceSearch extends DefaultSearch {

    private final StarRTree objectsOfInterest;
    private final PreferenceIndex preferenceIndex;
    private final boolean group;
    private final double radius;
    private final int neighborhood;

    public PreferenceSearch(StatisticCenter statisticCenter, boolean debug, Vocabulary termVocabulary,
            int numKeywords, int numResults, int numQueries, double alpha,
            double spaceMaxValue, String queryType, String customKeywords,
            int numMostFrequentTerms, int numWarmUpQueries, PreferenceIndex s2i, StarRTree objectsOfInterest,
            int neighborhood, boolean group, double radius) {
        super(statisticCenter, debug, termVocabulary, numKeywords,
                numResults, numQueries, alpha, spaceMaxValue, queryType,
                customKeywords, numMostFrequentTerms, numWarmUpQueries);
        this.preferenceIndex = s2i;
        this.objectsOfInterest = objectsOfInterest;
        this.group = group;
        this.neighborhood = neighborhood;
        this.radius = radius;
    }

    @Override
    public void open() throws ExperimentException {
        super.open();
        try {
            preferenceIndex.open();
            objectsOfInterest.open();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    @Override
    protected Iterator<SpatioTextualObject> execute(double queryLatitude,
            double queryLongitude, double maxDist, String queryKeywords, int k,
            double alpha) throws ExperimentException {
        try {
            if(debugMode){
                String msg = neighborhood==0?"[nn]":neighborhood==1?"[range]":"[influence]";  
                msg = msg + (group?"[PlusMode]":"");
                System.out.print(msg);
            }
            long time = System.currentTimeMillis();
            TreeSet<SpatioTextualObject> topK = new TreeSet<>(); //ordenada crescentemente

            Vector queryVector = new Vector();
            Vector.vectorize(queryVector, queryKeywords, termVocabulary);
            double queryLength = Vector.computeQueryWeight(queryVector, preferenceIndex.getDatasetSize(), preferenceIndex.getTermInfo());

            Cursor leaves = objectsOfInterest.query(1);
            while (leaves.hasNext()) {
                SpatioTreeHeapEntry leafEntry = new SpatioTreeHeapEntry(leaves.next());

                if (group) {

                    Cursor interestPointer = objectsOfInterest.query(leafEntry.getMBR());
                    List<ScoredObject> list = new LinkedList<>(); //objects of interest, conjunto V

                    while (interestPointer.hasNext()) {
                        SpatioTreeHeapEntry point = new SpatioTreeHeapEntry(interestPointer.next());
                        list.add(new ScoredObject(point.getId(),
                                point.getMBR().getCorner(false).getValue(0),
                                point.getMBR().getCorner(false).getValue(1)));
                    }
                    interestPointer.close();

                    if (neighborhood == 0) {//nn
                        preferenceIndex.nnQueryGroup(leafEntry.getMBR(), list, queryVector, queryLength);
                    } else if (neighborhood == 1) {//range
                        preferenceIndex.rangeQueryGroup(leafEntry.getMBR(), list, queryVector, queryLength, radius);
                    } else if (neighborhood == 2) {//influence
                        preferenceIndex.influenceQueryGroup(leafEntry.getMBR(), list, queryVector, queryLength, radius);
                    } else {
                        throw new ExperimentException("The neighborhood " + neighborhood + "' is not defined yet!!!");
                    }

                    for (ScoredObject obj : list) {
                        if (topK.size() < k) {
                            topK.add(obj);
                        } else if (obj.getScore() > topK.first().getScore()
                                || // keep the best objects, if they have the same scores, keeps the objects with smaller ids
                                (obj.getScore() == topK.first().getScore() && obj.getId() > topK.first().getId())) {
                            topK.pollFirst();
                            topK.add(obj);
                        }
                    }

                } else {
                    Cursor interestPointer = objectsOfInterest.query(leafEntry.getMBR());

                    while (interestPointer.hasNext()) {
                        SpatioTreeHeapEntry point = new SpatioTreeHeapEntry(interestPointer.next());
                        ScoredObject obj = new ScoredObject(point.getId(),
                                point.getMBR().getCorner(false).getValue(0),
                                point.getMBR().getCorner(false).getValue(1));
                        

                        if (neighborhood == 0) {//nn
                             preferenceIndex.nnQuery(obj, queryVector, queryLength);
                        } else if (neighborhood == 1) {//range                           
                            preferenceIndex.rangeQuery(obj, queryVector, queryLength, radius);
                        } else if (neighborhood == 2) {//influence
                            preferenceIndex.influenceQuery(obj, queryVector, queryLength, radius);
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
            }
            leaves.close(); 
            statisticCenter.getTally("avgQueryProcessingTime").update(
                    System.currentTimeMillis() - time);
            return topK.descendingIterator();
        } catch (Exception ex) {
            throw new ExperimentException(ex);
        }
    }

    @Override
    public void close() throws ExperimentException {
        try {
            termVocabulary.close();
            preferenceIndex.close();
            objectsOfInterest.close();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    @Override
    protected int getDocumentFrequency(int termId) {
        try {
            return preferenceIndex.getDocumentFrequency(termId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected int getTotalNumObjects() {
        try {
            return preferenceIndex.getDatasetSize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void collectStatistics(int count) {
        long treePageFaults = statisticCenter.getCount("trees_pageFaults").getValue();


        statisticCenter.getTally("avgTreePageFault").update(treePageFaults / (double) count);

        long filePageFaults = statisticCenter.getCount("fileTermManager_map_blocksRead").getValue();

        statisticCenter.getTally("avgFilePageFault").update(filePageFaults / (double) count);

        statisticCenter.getTally("avgPageFault").update((treePageFaults + filePageFaults) / (double) count);

        long treeNodesAccessed = statisticCenter.getCount("trees_nodesAccessed").getValue();

        statisticCenter.getTally("avgTreeNodesAccessed").update(treeNodesAccessed / (double) count);

        long fileBlocksAccessed = statisticCenter.getCount("fileTermManagerBlocksAccessed").getValue();

        statisticCenter.getTally("avgFileBloksAcessed").update(fileBlocksAccessed / (double) count);
    }

    public static void main(String[] args) throws Exception {
        Properties properties = Settings.loadProperties("framework.properties");

        DefaultStatisticCenter statistics = new DefaultStatisticCenter();

        PreferenceTreeTermManager treeTermManager = new PreferenceTreeTermManager(statistics, properties.getProperty("s2i.folder"),
                Integer.parseInt(properties.getProperty("s2i.tree.dimensions")),
                Integer.parseInt(properties.getProperty("s2i.tree.cacheSize")),
                Integer.parseInt(properties.getProperty("s2i.blockSize")),
                Integer.parseInt(properties.getProperty("s2i.tree.minNodeCapacity")),
                Integer.parseInt(properties.getProperty("s2i.tree.maxNodeCapacity")),
                Integer.parseInt(properties.getProperty("s2i.treesOpen")));

        PreferenceFileTermManager fileTermManager = new PreferenceFileTermManager(statistics,
                Integer.parseInt(properties.getProperty("s2i.blockSize")),
                properties.getProperty("s2i.folder") + "/s2i",
                Integer.parseInt(properties.getProperty("s2i.fileCacheSize")));

        PreferenceIndex prefIndex = new PreferenceIndex(statistics,
                properties.getProperty("s2i.folder"),
                Integer.parseInt(properties.getProperty("s2i.blockSize")),
                Integer.parseInt(properties.getProperty("s2i.treesOpen")),
                fileTermManager, treeTermManager,
                false/*construction time*/);

        StarRTree rTree = new StarRTree(statistics, "",
                properties.getProperty("s2i.folder") + "/rtree",
                Integer.parseInt(properties.getProperty("srtree.dimensions")),
                Integer.parseInt(properties.getProperty("srtree.cacheSize")),
                Integer.parseInt(properties.getProperty("disk.blockSize")),
                Integer.parseInt(properties.getProperty("srtree.tree.minNodeCapacity")),
                Integer.parseInt(properties.getProperty("srtree.tree.maxNodeCapacity")));

        LoadRTree.load(rTree, properties.getProperty("dataset.objectsFile"));

        prefIndex.open();//create the vocabulary
        PreferenceSearch searching = new PreferenceSearch(statistics,
                Boolean.parseBoolean(properties.getProperty("experiment.debug")),
                prefIndex.getTermVocabulary(),
                Integer.parseInt(properties.getProperty("query.numKeywords")),
                Integer.parseInt(properties.getProperty("query.numResults")),
                Integer.parseInt(properties.getProperty("query.numQueries")),
                Double.parseDouble(properties.getProperty("query.alfa")),
                Double.parseDouble(properties.getProperty("dataset.spaceMaxValue")),
                properties.getProperty("query.type"),
                properties.getProperty("query.keywords"),
                Integer.parseInt(properties.getProperty("query.numMostFrequentTerms")),
                Integer.parseInt(properties.getProperty("query.numWarmUpQueries")),
                prefIndex, rTree,
                Integer.parseInt(properties.getProperty("query.neighborhood")),
                Boolean.parseBoolean(properties.getProperty("experiment.plusMode")),
                Double.parseDouble(properties.getProperty("query.radius")));

        searching.open();
        searching.run();
        searching.close();            
        
        System.out.println("\n\nStatistics:\n" + statistics.getStatus());       
        
        Writer file = new OutputStreamWriter(new FileOutputStream(properties.getProperty("experiment.name") + ".txt", true), "utf-8");
      
        //colocar pageFault
        if (!Boolean.parseBoolean(properties.getProperty("experiment.append"))) {
            file.write("#queryTerms K range dataset blocosAcessados trees_nodesAccessed pageFaults tempo_resposta(ms)\n");
        }

        file.write(Integer.parseInt(properties.getProperty("query.numKeywords")) + " " + Integer.parseInt(properties.getProperty("query.numResults")) + " " 
                + Double.parseDouble(properties.getProperty("query.radius")) + " " + properties.getProperty("experiment.database") + " "
                + statistics.getCount("fileTermManagerEntriesAccessed").getValue() + " " + statistics.getCount("trees_nodesAccessed").getValue() + " "
                + " " + statistics.getCount("pageFaults").getValue() + " " + statistics.getTally("avgQueryProcessingTime").getMean() + "\n");
        file.close();
    }          
}
