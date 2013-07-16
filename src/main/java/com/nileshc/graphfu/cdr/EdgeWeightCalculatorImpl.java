/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr;

import com.nileshc.graphfu.cdr.normalizeids.translateedge.EdgeWeightCalculator;
import java.util.StringTokenizer;

/**
 *
 * @author nilesh
 */
public class EdgeWeightCalculatorImpl implements EdgeWeightCalculator {

    public double getWeightFor(String edgeData) {
        StringTokenizer tokenzier = new StringTokenizer(edgeData);
        int count = Integer.parseInt(tokenzier.nextToken());
        int totalVolume = Integer.parseInt(tokenzier.nextToken());
        int maxVolume = Integer.parseInt(tokenzier.nextToken());
        int minVolume = Integer.parseInt(tokenzier.nextToken());

        return totalVolume + count * (maxVolume + minVolume);
    }

    private static int tryParse(String text) {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
