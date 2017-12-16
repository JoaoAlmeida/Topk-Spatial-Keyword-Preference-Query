/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;
import java.util.Iterator;
import s2i.SpatioItem;
import util.nra.NRAItem;

/**
 *
 * @author JoãoPaulo
 */
public class SpatioItemCollection extends NRAItem {

    private ArrayList<SpatioItem> collection;

    public SpatioItemCollection() {
        collection = new ArrayList<>();
    }

    public SpatioItem get(int index) {
        return collection.get(index);
    }

    public void add(SpatioItem item) {
        collection.add(item);
    }

    public int size() {
        return collection.size();
    }

    public boolean isEmpty() {
        return collection.isEmpty();
    }

    public void clear() {
        collection.clear();
    }

    public Iterator iterator() {
        return collection.iterator();
    }
}
