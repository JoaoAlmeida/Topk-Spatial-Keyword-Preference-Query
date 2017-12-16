/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;

import util.SpatioTextualObject;
import java.util.Iterator;
import java.util.Properties;
import util.DefaultSearch;
import util.config.Settings;
import util.experiment.ExperimentException;
import util.sse.Vocabulary;
import util.statistics.DefaultStatisticCenter;
import util.statistics.StatisticCenter;

/**
 *
 * @author joao
 */
public class S2ISearch extends DefaultSearch {

    private final SpatialInvertedIndex s2i;

    public S2ISearch(StatisticCenter statisticCenter, boolean debug, Vocabulary termVocabulary,
            int numKeywords, int numResults, int numQueries, double alpha,
            double spaceMaxValue, String queryType, String customKeywords, int numMostFrequentTerms,
            int numWarmUpQueries, SpatialInvertedIndex s2i) {
        super(statisticCenter, debug, termVocabulary, numKeywords,
                numResults, numQueries, alpha, spaceMaxValue, queryType,
                customKeywords, numMostFrequentTerms, numWarmUpQueries);
        this.s2i = s2i;
    }

    @Override
    public void open() throws ExperimentException {
        super.open();
    }

    @Override
    protected Iterator<SpatioTextualObject> execute(double queryLatitude,
            double queryLongitude, double maxDist, String queryKeywords, int k,
            double alpha) throws ExperimentException {
        try {
            Iterator<SpatioTextualObject> queryResult = null;
            long time = System.currentTimeMillis();

            queryResult = s2i.search(queryLatitude, queryLongitude, maxDist, queryKeywords, k, alpha);

            statisticCenter.getTally("avgQueryProcessingTime").update(
                    System.currentTimeMillis() - time);
            return queryResult;
        } catch (Exception ex) {
            throw new ExperimentException(ex);
        }
    }

    @Override
    protected int getDocumentFrequency(int termId) {
        try {
            return s2i.getDocumentFrequency(termId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected int getTotalNumObjects() {
        try {
            return s2i.getDatasetSize();
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

    @Override
    public void close() throws ExperimentException {
        try {
            this.s2i.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        Properties properties = Settings.loadProperties("s2i.properties");

        DefaultStatisticCenter statistics = new DefaultStatisticCenter();
        

        TreeTermManager treeTermManager =  new TreeTermManager(statistics, properties.getProperty("s2i.folder"),
                    Integer.parseInt(properties.getProperty("s2i.tree.dimensions")), 
                    Integer.parseInt(properties.getProperty("s2i.tree.cacheSize")),
                    Integer.parseInt(properties.getProperty("s2i.blockSize")), 
                    Integer.parseInt(properties.getProperty("s2i.tree.minNodeCapacity")),
                    Integer.parseInt(properties.getProperty("s2i.tree.maxNodeCapacity")), 
                    Integer.parseInt(properties.getProperty("s2i.treesOpen")));

        FileTermManager fileTermManager = new FileTermManager(statistics,
                Integer.parseInt(properties.getProperty("s2i.blockSize")),
                properties.getProperty("s2i.folder") + "/s2i",
                Integer.parseInt(properties.getProperty("s2i.fileCacheSize")));

        SpatialInvertedIndex s2i = new SpatialInvertedIndex(statistics,
                properties.getProperty("s2i.folder"),
                Integer.parseInt(properties.getProperty("s2i.blockSize")),
                Integer.parseInt(properties.getProperty("s2i.treesOpen")),
                fileTermManager, treeTermManager,
                false /*construction time*/);

        s2i.open();

        S2ISearch searching = new S2ISearch(statistics,
                Boolean.parseBoolean(properties.getProperty("experiment.debug")),
                s2i.getTermVocabulary(),
                Integer.parseInt(properties.getProperty("query.numKeywords")),
                Integer.parseInt(properties.getProperty("query.numResults")),
                Integer.parseInt(properties.getProperty("query.numQueries")),
                Double.parseDouble(properties.getProperty("query.alfa")),
                Double.parseDouble(properties.getProperty("dataset.spaceMaxValue")),
                properties.getProperty("query.type"),
                properties.getProperty("query.keywords"),
                Integer.parseInt(properties.getProperty("query.numMostFrequentTerms")),
                Integer.parseInt(properties.getProperty("query.numWarmUpQueries")),
                s2i);

        searching.open();
        searching.run();
        searching.close();

        System.out.println("\n\nStatistics:\n" + statistics.getStatus());
    }
}
