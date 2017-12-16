/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package s2i;

import dataset.DatasetTupleFactory;
import dataset.SpatialKeywordTuple;
import java.util.Date;
import java.util.Properties;
import util.Util;
import util.config.Settings;
import util.experiment.Experiment;
import util.experiment.ExperimentException;
import util.experiment.ExperimentResult;
import util.experiment.StringExperimentResult;
import util.sse.Vector;
import util.statistics.DefaultStatisticCenter;
import util.statistics.StatisticCenter;

/**
 *
 * @author joao
 */
public class S2IBuildIndex implements Experiment {

    protected static final long STATUS_TIME = 2 * 60 * 1000;
    private final DatasetTupleFactory tupleFactory;
    private final StatisticCenter statisticCenter;
    private final SpatialInvertedIndex index;
    private final int maxNumTuples;

    public S2IBuildIndex(StatisticCenter statisticCenter, DatasetTupleFactory tupleFactory,
            int maxNumTuples, SpatialInvertedIndex index) {
        this.statisticCenter = statisticCenter;
        this.tupleFactory =  tupleFactory;
        this.index = index;
        this.maxNumTuples = maxNumTuples;
    }

    public void open() throws ExperimentException {
        try {
            index.open();
            tupleFactory.open();
        } catch (Exception ex) {
            throw new ExperimentException(ex);
        }
    }

    public void run() throws ExperimentException {
        try {
            long time = System.currentTimeMillis();

            if (index.getNumDistinctTerms() > 0) {
                System.out.println("The term vocabulary already exists! You have to clean the S2I output directory before calling the BuildIndex.");
                return;
            }

            int count = 0;
            SpatialKeywordTuple tuple = (SpatialKeywordTuple) tupleFactory.produce();

            while (tuple != null && count < maxNumTuples) {
                if (Vector.hasTokens(tuple.getText())) {
                    count++;

                    index.insert(tuple.getId(), tuple.getValue(0), tuple.getValue(1), tuple.getText());

                    statisticCenter.getCount("datasetLines").inc();
                    statisticCenter.getTally("datasetLineLength").update(tuple.getText().length());

                    if ((System.currentTimeMillis() - time) > STATUS_TIME) {
                        time = System.currentTimeMillis();
                        System.out.print(" [" + count + "]");
                        //System.out.println(Util.time(System.currentTimeMillis()-start)+".");
                    }
                }
                tuple = (SpatialKeywordTuple) tupleFactory.produce();
                if(count == maxNumTuples){
                    System.out.println("MaxTuples alcaçado!");
                }
            }
            time = System.currentTimeMillis();
            System.out.print("\nBuilding trees..." + new Date());
            int numTrees = index.buildTrees(STATUS_TIME);
            System.out.println(", concluded in " + Util.time(System.currentTimeMillis() - time));

            time = System.currentTimeMillis();
            System.out.print("Flushing the tree..." + new Date());
            index.flush();
            System.out.println(", concluded in " + Util.time(System.currentTimeMillis() - time));

            statisticCenter.getCount("numDistinctTerms").update(index.getNumDistinctTerms());
            statisticCenter.getCount("numTrees").update(numTrees);
            statisticCenter.getCount("numFileEntries").update(index.getNumDistinctTerms() - numTrees);
            statisticCenter.getCount("datasetSize").update(count);
            statisticCenter.getCount("FileManagerSizeInBytes").update(index.getFileManagerSizeInBytes());
            statisticCenter.getCount("TreeManagerSizeInBytes").update(index.getTreeManagerSizeInBytes());
            statisticCenter.getCount("S2ISizeInBytes").update(index.getSizeInBytes());
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    public void close() throws ExperimentException {
        try {
            index.close();
            tupleFactory.close();
        } catch (Exception ex) {
            throw new ExperimentException(ex);
        }
    }

    public ExperimentResult[] getResult() {
        return new ExperimentResult[]{new StringExperimentResult(1, "Index built!")};
    }

    public static void main(String[] args) throws Exception {
        Properties properties = Settings.loadProperties("framework.properties");

        DefaultStatisticCenter statistics = new DefaultStatisticCenter();

        TreeTermManager treeTermManager = new TreeTermManager(statistics, properties.getProperty("experiment.folder"),
                Integer.parseInt(properties.getProperty("s2i.tree.dimensions")),
                Integer.parseInt(properties.getProperty("s2i.treesOpen")),
                Integer.parseInt(properties.getProperty("disk.blockSize")),
                Integer.parseInt(properties.getProperty("s2i.tree.minNodeCapacity")),
                Integer.parseInt(properties.getProperty("s2i.tree.maxNodeCapacity")),
                Integer.parseInt(properties.getProperty("s2i.tree.cacheSize")));

        FileTermManager fileTermManager = new FileTermManager(statistics,
                Integer.parseInt(properties.getProperty("disk.blockSize")),
                properties.getProperty("experiment.folder") + "/s2i",
                Integer.parseInt(properties.getProperty("s2i.fileCacheSize")));

        SpatialInvertedIndex s2i = new SpatialInvertedIndex(statistics,
                properties.getProperty("experiment.folder"),
                Integer.parseInt(properties.getProperty("disk.blockSize")),
                Integer.parseInt(properties.getProperty("s2i.treesOpen")),
                fileTermManager, treeTermManager,
                true /*construction time*/);


        S2IBuildIndex indexing = new S2IBuildIndex(statistics,
                new DatasetTupleFactory(properties.getProperty("dataset.featuresFile")),
                Integer.parseInt(properties.getProperty("dataset.maxNumTuples")), s2i);

        long time = System.currentTimeMillis();

        indexing.open();
        indexing.run();
        indexing.close();

        System.out.println("Indexing finished in " + Util.time(System.currentTimeMillis() - time));

        System.out.println("\n\nStatistics:\n" + statistics.getStatus());
    }
}
