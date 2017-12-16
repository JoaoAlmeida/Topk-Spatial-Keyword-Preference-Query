package invertedFile;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import util.IFTuple;
import util.config.Settings;
import util.experiment.Experiment;
import util.experiment.ExperimentException;
import util.experiment.ExperimentResult;
import util.experiment.StringExperimentResult;
import util.file.BufferedListStorage;
import util.file.EntryStorage;
import util.file.IntegerEntry;
import util.sse.Term;
import util.sse.Vector;
import util.sse.Vocabulary;
import util.statistics.DefaultStatisticCenter;
import util.statistics.StatisticCenter;

public class Indexer implements Experiment {

    private final String termInfoFile;
    private final String docLengthFile;
    private final BufferedListStorage<IFTuple> cache;
    private final Vocabulary dictionary;
    private HashMap<Integer, Integer> documentLength;
    private final String inputDataset;
    private final StatisticCenter statisticCenter;
    //maps term id (from termVocabulary) to the number of documents that has the term (document frequency of the term)
    protected EntryStorage<IntegerEntry> termInfo;

    public Indexer(StatisticCenter statisticCenter, String statsId,
            int cacheSize, String inputDataset, String dictionary,
            String ifFiles, String docLengthFile, String termInfoFile) throws FileNotFoundException {

        this.documentLength = new HashMap<>();
        this.dictionary = new Vocabulary(dictionary);
        this.cache = new BufferedListStorage<>(statisticCenter, statsId, ifFiles, cacheSize, IFTuple.SIZE, IFTuple.FACTORY);
        this.inputDataset = inputDataset;
        this.statisticCenter = statisticCenter;
        this.docLengthFile = docLengthFile;
        this.termInfoFile = termInfoFile;
    }

    public Object[] getSubstrings(String featureLine) {
        Object[] subs = new Object[4];
        int pos = featureLine.indexOf(' ');
        int docID = Integer.parseInt(featureLine.substring(0, pos));
        int pos2 = featureLine.indexOf(' ', pos + 1);
        double lat = Double.parseDouble(featureLine.substring(pos + 1, pos2));
        pos = pos2;
        pos2 = featureLine.indexOf(' ', pos + 1);
        double lgt = Double.parseDouble(featureLine.substring(pos + 1, pos2));
        String dsc = featureLine.substring(pos2 + 1).toLowerCase();
        subs[0] = docID;
        subs[1] = lat;
        subs[2] = lgt;
        subs[3] = dsc;
        return subs;
    }

    @Override
    public void run() throws ExperimentException {
        try {
            BufferedReader featureReader = new BufferedReader((new InputStreamReader(new FileInputStream(inputDataset), "utf-8")));
            String featureLine = featureReader.readLine();
            int cont = 0;

            long time = System.currentTimeMillis();
            while (featureLine != null) {
                if (System.currentTimeMillis() - time >= 10000) {
                    System.out.println("Indexing line " + cont + "...");
                    time = System.currentTimeMillis();
                }
                cont++;

                int pos;
                int docID = Integer.parseInt(featureLine.substring(0, pos = featureLine.indexOf(' ')));
                double lat = Double.parseDouble(featureLine.substring(pos + 1, pos = featureLine.indexOf(' ', pos + 1)));
                double lon = Double.parseDouble(featureLine.substring(pos + 1, pos = featureLine.indexOf(' ', pos + 1)));
                String text = featureLine.substring(pos + 1);


                if (Vector.hasTokens(text)) {
                    Vector vector = new Vector(docID);
                    int numWords = Vector.vectorize(vector, text, dictionary);
                    double docLength = Vector.computeDocWeight(vector);

                    statisticCenter.getCount("totalNumberOfWords").update(numWords);
                    statisticCenter.getTally("avgNumDistinctTerms").update(vector.size());
                    statisticCenter.getCount("datasetLines").inc();
                    statisticCenter.getTally("datasetLineLength").update(text.length());

                    this.documentLength.put(docID, numWords);

                    Iterator<Term> it = vector.iterator();
                    while (it.hasNext()) {
                        Term term = it.next();

                        //We normalize weight by the document lenght, which gives the impact of t
                        term.setWeight(term.getWeight() / docLength);

                        //add in each inverted list of a term, the new entry <DocId, termImpact>
                        cache.addEntry(term.getTermId(), new IFTuple(vector.getId(), term.getWeight(), lat, lon), false);


                        //Update the df (document frequency) of the term
                        IntegerEntry entry = termInfo.getEntry(term.getTermId());
                        termInfo.putEntry(term.getTermId(), entry == null ? new IntegerEntry(1)
                                : new IntegerEntry(entry.getValue() + 1));
                    }
                }


                featureLine = featureReader.readLine();
            }
            featureReader.close();

            statisticCenter.getCount("numDistinctTerms").update(dictionary.size());
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    @Override
    public void open() throws ExperimentException {
        try {
            this.cache.open();
            this.dictionary.open();

            termInfo = new EntryStorage<IntegerEntry>(statisticCenter, "termInfo",
                    termInfoFile, IntegerEntry.SIZE, IntegerEntry.FACTORY);
            termInfo.open();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }

    @Override
    public void close() throws ExperimentException {
        try {
            this.cache.close();
            this.dictionary.close();

            FileOutputStream fileOutputStream = new FileOutputStream(this.docLengthFile);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            objectOutputStream.writeObject(this.documentLength);
            objectOutputStream.close();

            termInfo.close();
        } catch (Exception e) {
            throw new ExperimentException(e);
        }
    }
    
    @Override
    public ExperimentResult[] getResult() {
        return new ExperimentResult[]{new StringExperimentResult(1, "Index built!")};
    }

    public static void main(String[] args) throws Exception{
        Properties properties = Settings.loadProperties("framework.properties");
        DefaultStatisticCenter stats = new DefaultStatisticCenter();
        String folder = properties.getProperty("experiment.folder");
        Indexer indexer;
        indexer = new Indexer(
                stats, "invertedFile",
                Integer.parseInt(properties.getProperty("if.cacheSize")),
                properties.getProperty("dataset.featuresFile"),
                folder + "/" + properties.getProperty("if.vocabulary"),
                folder + "/ifList",
                folder + "/" + properties.getProperty("if.docLengthFile"),
                folder + "/" + properties.getProperty("if.termInfoFile"));

        indexer.open();
        indexer.run();
        indexer.close();

        System.out.println(stats.getStatus());
    }    
}
