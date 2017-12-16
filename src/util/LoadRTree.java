/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import xxl.core.io.converters.IntegerConverter;
import xxl.core.spatial.KPE;
import xxl.core.spatial.rectangles.DoublePointRectangle;
import xxl.util.StarRTree;
import static xxl.util.StarRTree.getValues;

/**
 *
 * @author Robos
 */
public class LoadRTree {

    public static double[] getValues(String line, int dims) {
        StringTokenizer tokens = new StringTokenizer(line);

        double[] values = new double[dims];
        int d = 0;
        String token = null;

        token = tokens.nextToken();//descarta o ID

        for (int i = 0; d < dims && tokens.hasMoreTokens(); i++) {
            token = tokens.nextToken();
            values[d] = Double.parseDouble(token);
            d++;
        }

        return values;
    }

    public static void load(StarRTree rTree, String sourceFileName) throws FileNotFoundException, IOException, ClassNotFoundException {
        rTree.open();

        if (rTree.getSizeInBytes() > 0) {
            System.out.println("The RTree is already loaded!!! ");
        } else {
            System.out.print("The RTree is empty, loading RTree with data extracted from "+sourceFileName+"...");
            int count=0;
            BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(sourceFileName)));
            String line = input.readLine();
            double[] point;
            for (int i = 1; line != null; i++) {
                count++;
                point = getValues(line, 2);
                DoublePointRectangle mbr = new DoublePointRectangle(point, point);
                //DoublePointRectangleMax mbr = new DoublePointRectangleMax(point, point, RandomUtil.nextDouble(1));
                rTree.insert(new KPE(mbr, i, IntegerConverter.DEFAULT_INSTANCE));
                line = input.readLine();
            }
            input.close();
            System.out.println(count+" objects inserted!");
        }
        rTree.close();
    }
}
