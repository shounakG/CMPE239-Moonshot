package edu.sjsu.cmpe239.moonshot;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

public class GraphStream {
    public static void main(String args[]) {
        Graph graph = new SingleGraph("Tutorial 1");
        graph.addNode("A" );
        graph.addNode("B" );
        graph.addNode("C" );
        graph.addEdge("AB", "A", "B");
        graph.addEdge("BC", "B", "C");
        graph.addEdge("CA", "C", "A");
        graph.display();
    }
}
