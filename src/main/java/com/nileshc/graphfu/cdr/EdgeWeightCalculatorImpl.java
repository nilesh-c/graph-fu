/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr;

import com.nileshc.graphfu.cdr.normalizeids.translateedge.EdgeWeightCalculator;

/**
 *
 * @author nilesh
 */
public class EdgeWeightCalculatorImpl implements EdgeWeightCalculator {

    public double getWeightFor(String edgeData) {
        return Double.parseDouble(edgeData);
    }
}
