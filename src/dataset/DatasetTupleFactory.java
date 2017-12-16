package dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import util.tuples.Cell;
import util.tuples.Tuple;
import util.tuples.TupleFactory;

public class DatasetTupleFactory implements TupleFactory {

    private final String inputFileName;
    private BufferedReader reader;

    public DatasetTupleFactory(String inputFileName) {
        this.inputFileName = inputFileName;
    }

    public void open() throws FileNotFoundException, UnsupportedEncodingException {
        reader = new BufferedReader((new InputStreamReader(new FileInputStream(new File(inputFileName)), "UTF-8")));
    }

    public void close() throws IOException {
        reader.close();
    }

    //alteração feita aqui
    @Override
    public Tuple produce() {
        int pos;
        int objectId;
        double[] coordinates = new double[2];
        String text;

        String line;
        try {
            line = reader.readLine();

            if (line != null) {
                //parseLine
                try {
                    objectId = Integer.parseInt(line.substring(0, (pos = line.indexOf(' '))));


                    coordinates[0] = Double.parseDouble(line.substring(pos + 1, pos = line.indexOf(' ', pos + 1)));
                    coordinates[1] = Double.parseDouble(line.substring(pos + 1, (pos = line.indexOf(' ', pos + 1))));

                    text = line.substring(pos + 1);

                } catch (NumberFormatException e) {
                    System.out.println("Invalid line='" + line + "'!!!");
                    return produce();
                }
                return new SpatialKeywordTuple(coordinates, text.toString(), objectId);
            } else { //end of file reached
                return null;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
         
    @Override
    public Cell[] getClustersMBRs() {
        return null;
    }
}
